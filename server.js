require('dotenv').config();
const express = require('express');
const puppeteer = require('puppeteer');
const Anthropic = require('@anthropic-ai/sdk');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const sessions = new Map();
const sseClients = new Map();

app.get('/api/stream/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write('\n');
  if (!sseClients.has(sessionId)) sseClients.set(sessionId, []);
  sseClients.get(sessionId).push(res);
  req.on('close', () => {
    const clients = sseClients.get(sessionId) || [];
    sseClients.set(sessionId, clients.filter((c) => c !== res));
  });
});

function emit(sessionId, event, data) {
  const clients = sseClients.get(sessionId) || [];
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  clients.forEach((c) => c.write(payload));
}

// Extract text from Anthropic API response (handles different content block types)
function extractResponseText(response) {
  if (!response || !response.content || !Array.isArray(response.content)) {
    console.error('Unexpected response structure:', JSON.stringify(response, null, 2));
    return null;
  }
  // Find the first text block
  for (const block of response.content) {
    if (block.type === 'text' && typeof block.text === 'string') {
      return block.text.trim();
    }
  }
  // Fallback: try to get text from any block that has a text property
  for (const block of response.content) {
    if (typeof block.text === 'string') {
      return block.text.trim();
    }
  }
  // Last resort: stringify the first block
  console.error('No text block found in response. Content blocks:', JSON.stringify(response.content, null, 2));
  return null;
}

const SYSTEM_PROMPT = `You are an AI browser automation agent. You look at screenshots of web pages and decide what action to take next to accomplish the user's goal.

You MUST respond with EXACTLY ONE JSON object (no markdown, no explanation) in this format:

{
  "thought": "Brief reasoning about what you see and what to do next",
  "action": "<action_type>",
  "params": { ... },
  "done": false
}

Available actions:
- "click": Click an element. params: { "x": <number>, "y": <number> }
- "type": Type text into the focused element. params: { "text": "<string>" }
- "navigate": Go to a URL. params: { "url": "<string>" }
- "scroll": Scroll the page. params: { "direction": "up" | "down", "amount": 300 }
- "wait": Wait for page to load. params: { "ms": 2000 }
- "press": Press a keyboard key. params: { "key": "Enter" | "Tab" | "Escape" }
- "done": Task is complete. params: { "summary": "<result summary>" }

When you see a login page:
1. First click on the username/email input field
2. Then type the username
3. Then click on the password field
4. Then type the password
5. Then click the login/submit button

The screenshot is 1280x720. Coordinates are in pixels from top-left.
Always set "done": true when the task is complete or when you determine it cannot be completed.`;

async function runAgentLoop(sessionId, task, maxSteps = 15) {
  const session = sessions.get(sessionId);
  if (!session) return;

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--window-size=1280,720'],
    defaultViewport: { width: 1280, height: 720 },
  });

  session.browser = browser;
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
  emit(sessionId, 'status', { message: 'Browser launched', step: 0 });

  const history = [];

  for (let step = 1; step <= maxSteps; step++) {
    if (session.cancelled) {
      emit(sessionId, 'status', { message: 'Task cancelled', step });
      break;
    }
    try {
      const screenshotBuffer = await page.screenshot({ encoding: 'base64' });
      emit(sessionId, 'screenshot', { image: screenshotBuffer, step, url: page.url() });

      const messages = [
        ...history,
        {
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: 'image/png', data: screenshotBuffer } },
            { type: 'text', text: step === 1
              ? `Task: ${task}\n\nThis is the current browser state. Decide the first action to take.`
              : `Here is the updated browser state after the last action. Current URL: ${page.url()}. Continue with the task: ${task}` },
          ],
        },
      ];

      emit(sessionId, 'status', { message: `Step ${step}: Analyzing page...`, step });

      const response = await anthropic.messages.create({
        model: 'claude-sonnet-5',
        max_tokens: 1024,
        system: SYSTEM_PROMPT,
        messages,
      });

      console.log(`[Step ${step}] Response stop_reason: ${response.stop_reason}, content blocks: ${response.content?.length}`);

      const responseText = extractResponseText(response);
      if (!responseText) {
        emit(sessionId, 'error', { message: `Empty or unreadable AI response. Stop reason: ${response.stop_reason}. Content types: ${response.content?.map(b => b.type).join(', ')}`, step });
        break;
      }

      let decision;
      try {
        const jsonMatch = responseText.match(/\{[\s\S]*\}/);
        decision = JSON.parse(jsonMatch ? jsonMatch[0] : responseText);
      } catch (e) {
        emit(sessionId, 'error', { message: `Failed to parse AI response: ${responseText.substring(0, 200)}`, step });
        break;
      }

      emit(sessionId, 'action', { thought: decision.thought, action: decision.action, params: decision.params, step });

      history.push(
        { role: 'user', content: `[Step ${step}] Screenshot taken. Current URL: ${page.url()}` },
        { role: 'assistant', content: responseText }
      );

      if (decision.done || decision.action === 'done') {
        emit(sessionId, 'complete', { summary: decision.params?.summary || decision.thought, step });
        const finalScreenshot = await page.screenshot({ encoding: 'base64' });
        emit(sessionId, 'screenshot', { image: finalScreenshot, step, url: page.url(), final: true });
        break;
      }

      await executeAction(page, decision, sessionId, step);
      await new Promise((r) => setTimeout(r, 1500));
    } catch (err) {
      emit(sessionId, 'error', { message: err.message, step });
      break;
    }
  }

  try { await browser.close(); } catch (_) {}
  session.status = 'completed';
  emit(sessionId, 'status', { message: 'Session ended', step: -1 });
}

async function executeAction(page, decision, sessionId, step) {
  const { action, params } = decision;
  switch (action) {
    case 'navigate':
      emit(sessionId, 'status', { message: `Navigating to ${params.url}`, step });
      await page.goto(params.url, { waitUntil: 'networkidle2', timeout: 15000 });
      break;
    case 'click':
      emit(sessionId, 'status', { message: `Clicking at (${params.x}, ${params.y})`, step });
      await page.mouse.click(params.x, params.y);
      break;
    case 'type':
      emit(sessionId, 'status', { message: `Typing "${params.text.substring(0, 20)}${params.text.length > 20 ? '...' : ''}"`, step });
      await page.keyboard.type(params.text, { delay: 50 });
      break;
    case 'press':
      emit(sessionId, 'status', { message: `Pressing ${params.key}`, step });
      await page.keyboard.press(params.key);
      break;
    case 'scroll':
      emit(sessionId, 'status', { message: `Scrolling ${params.direction}`, step });
      await page.mouse.wheel({ deltaY: params.direction === 'down' ? (params.amount || 300) : -(params.amount || 300) });
      break;
    case 'wait':
      emit(sessionId, 'status', { message: `Waiting ${params.ms}ms`, step });
      await new Promise((r) => setTimeout(r, params.ms || 2000));
      break;
    default:
      emit(sessionId, 'status', { message: `Unknown action: ${action}`, step });
  }
}

app.post('/api/agent/start', (req, res) => {
  const { task } = req.body;
  if (!task) return res.status(400).json({ error: 'Task is required' });
  const sessionId = uuidv4();
  sessions.set(sessionId, { task, status: 'running', cancelled: false });
  runAgentLoop(sessionId, task).catch((err) => {
    emit(sessionId, 'error', { message: err.message, step: -1 });
  });
  res.json({ sessionId });
});

app.post('/api/agent/stop/:sessionId', (req, res) => {
  const session = sessions.get(req.params.sessionId);
  if (session) {
    session.cancelled = true;
    if (session.browser) session.browser.close().catch(() => {});
    res.json({ success: true });
  } else {
    res.status(404).json({ error: 'Session not found' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', version: '1.0.0' });
});

const PORT = process.env.PORT || 80;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`AI Browser Agent running on port ${PORT}`);
});
