# Monopoly online mod - room relay

A tiny WebSocket server that pairs players into rooms (one host + up to 3 guests) and forwards
their messages. It never looks at game data and keeps everything in memory.

## Run locally
    npm install
    node server.js          # listens on PORT (default 10000)
    node test.js            # smoke test (needs the server running)

## Deploy on Render
1. Put this folder in a Git repository (GitHub/GitLab) - only `server.js`, `package.json`, `render.yaml`.
2. Render dashboard > New > Blueprint (uses `render.yaml`) or New > Web Service:
   Runtime Node, Build `npm install --omit=dev`, Start `node server.js`, Health check `/healthz`.
3. Copy the service URL (e.g. `https://my-relay.onrender.com`) and give players this line for `online.cfg`
   next to the game: `relay=wss://my-relay.onrender.com`

The free plan sleeps after ~15 min idle; the first connection after that can take 30-60 s while it wakes up.
