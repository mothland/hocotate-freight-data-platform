// === INITIAL SETUP ===
const canvas = document.getElementById("map");
const ctx = canvas.getContext("2d");
canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

const center = { x: canvas.width / 2, y: canvas.height / 2 };
const SCALE = 4e-7;
let panX = 0, panY = 0;
let zoom = 3;
let time = 0;

// === PLANET DATA ===
const PLANETS = {
  "Hocotate": [0.0, 0.0],
  "PNF-404": [0.7e8, -1.3e8],
  "Karut": [1.2e8, 0.8e8],
  "Giya": [-1.5e8, 0.3e8],
  "Nijo": [0.7e8, -1.1e8],
  "Sozor": [-0.9e8, -0.6e8],
  "Koppai": [6.8e8, 2.1e8],
  "Ohri": [5.2e8, -2.4e8],
  "Moyama": [-6.1e8, 3.5e8],
  "Flukuey": [4.0e8, -3.8e8],
  "Enohay": [3.3e8, 1.9e8],
  "Mihama": [-3.7e8, -1.6e8],
  "Ooji": [7.5e8, 0.0e8],
  "Ogura": [-5.5e8, 2.2e8],
  "Conohan": [9.4e8, -4.1e8],
  "Ocobo": [-1.0e9, 3.8e8],
  "Tagwa": [1.12e9, 1.1e8],
  "Enohee": [-1.25e9, -0.2e9],
  "Neechki": [1.35e9, -3.0e8],
  "Koodgio": [-1.10e9, 4.5e8],
  "Maxima": [1.45e9, 0.0e8]
};

// === COORDINATE HELPERS ===
function toScreen(x, y) {
  return [
    center.x + (x * SCALE * zoom) + panX,
    center.y - (y * SCALE * zoom) - panY
  ];
}

// === BACKGROUND ===
function drawStarfield() {
  ctx.fillStyle = "rgba(255,255,255,0.5)";
  for (let i = 0; i < 300; i++) {
    const x = Math.random() * canvas.width;
    const y = Math.random() * canvas.height;
    ctx.fillRect(x, y, 1, 1);
  }
}

function drawScanLines() {
  ctx.strokeStyle = "rgba(0, 255, 150, 0.03)";
  for (let i = 0; i < canvas.height; i += 4) {
    ctx.beginPath();
    ctx.moveTo(0, i);
    ctx.lineTo(canvas.width, i);
    ctx.stroke();
  }
}

function drawOrbits() {
  let maxDist = 0;
  for (const [_, [x, y]] of Object.entries(PLANETS)) {
    const dist = Math.sqrt(x * x + y * y);
    if (dist > maxDist) maxDist = dist;
  }

  const maxScreenRadius = maxDist * SCALE * zoom;
  const rings = 5;
  const step = maxScreenRadius / rings;
  const [hocotateX, hocotateY] = toScreen(0, 0);

  ctx.strokeStyle = "rgba(100,200,255,0.15)";
  ctx.lineWidth = 1.5;
  ctx.setLineDash([]);

  for (let i = 1; i <= rings; i++) {
    const r = i * step;
    ctx.beginPath();
    ctx.arc(hocotateX, hocotateY, r, 0, 2 * Math.PI);
    ctx.stroke();
  }

  ctx.strokeStyle = "rgba(0, 255, 150, 0.1)";
  ctx.setLineDash([5, 5]);
  ctx.beginPath();
  ctx.arc(hocotateX, hocotateY, step * 0.8, 0, 2 * Math.PI);
  ctx.stroke();
  ctx.setLineDash([]);
}

function drawPlanets() {
  ctx.font = "bold 13px 'Courier New'";
  ctx.textAlign = "center";

  for (const [name, [x, y]] of Object.entries(PLANETS)) {
    const [sx, sy] = toScreen(x, y);
    const pulse = Math.sin(time * 0.003) * 0.3 + 0.7;

    ctx.fillStyle = `rgba(100, 200, 255, ${0.3 * pulse})`;
    ctx.beginPath();
    ctx.arc(sx, sy, 12, 0, 2 * Math.PI);
    ctx.fill();

    ctx.fillStyle = "#64c8ff";
    ctx.beginPath();
    ctx.arc(sx, sy, 6, 0, 2 * Math.PI);
    ctx.fill();

    ctx.strokeStyle = "rgba(100, 200, 255, 0.6)";
    ctx.stroke();

    ctx.fillStyle = "#00ff99";
    ctx.fillText(name, sx, sy - 18);
  }
}

// === DYNAMIC SHIPS ===
let ships = [];
const fadingShips = {}; // {ship_id: opacity}

function drawShips() {
  ctx.font = "11px 'Courier New'";
  ctx.textAlign = "center";

  for (const s of ships) {
    const [sx, sy] = toScreen(s.coord_x, s.coord_y);

    // Determine color by status
    let color = "#ffaa00"; // OK = yellow
    if (s.status === "REPAIR") color = "#ff3333";
    if (s.status === "FAILURE") color = "#888888";

    // Handle fade-out animation
    const id = s.ship_id || s.ship || "?";
    let alpha = fadingShips[id] ?? 1.0;
    ctx.fillStyle = color;
    ctx.globalAlpha = alpha;
    ctx.beginPath();
    ctx.arc(sx, sy, 5, 0, 2 * Math.PI);
    ctx.fill();
    ctx.globalAlpha = 1.0; // reset alpha
    ctx.fillText(id, sx, sy - 10);
  }

  // Gradually fade out removed ships
  for (const [id, opacity] of Object.entries(fadingShips)) {
    if (!ships.some(s => s.ship_id === id)) {
      fadingShips[id] = Math.max(0, opacity - 0.02);
      if (fadingShips[id] === 0) delete fadingShips[id];
    }
  }
}

// === MAIN LOOP ===
function draw() {
  ctx.fillStyle = "rgba(10, 14, 39, 0.25)";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  drawStarfield();
  drawScanLines();
  drawOrbits();
  drawPlanets();
  drawShips();
  time++;
  requestAnimationFrame(draw);
}
draw();

// === MOUSE + ZOOM ===
let dragging = false, lastX = 0, lastY = 0;

canvas.addEventListener("mousedown", e => {
  dragging = true;
  lastX = e.clientX;
  lastY = e.clientY;
});
canvas.addEventListener("mouseup", () => dragging = false);
canvas.addEventListener("mouseleave", () => dragging = false);
canvas.addEventListener("mousemove", e => {
  if (dragging) {
    panX += e.clientX - lastX;
    panY += e.clientY - lastY;
    lastX = e.clientX;
    lastY = e.clientY;
  }
});

document.getElementById("zoomIn").addEventListener("click", () => {
  zoom = Math.min(zoom * 1.2, 10);
});
document.getElementById("zoomOut").addEventListener("click", () => {
  zoom = Math.max(zoom / 1.2, 0.1);
});

window.addEventListener("resize", () => {
  canvas.width = window.innerWidth;
  canvas.height = window.innerHeight;
  center.x = canvas.width / 2;
  center.y = canvas.height / 2;
});

// === LIVE RADAR FEED ===
const ws = new WebSocket("ws://localhost:8001/ws");

ws.onopen = () => console.log("📡 Connected to radar bridge");
ws.onmessage = (event) => {
  try {
    const newShips = JSON.parse(event.data);
    const ids = newShips.map(s => s.ship_id || s.ship);
    // Start fading for ships that disappeared
    for (const old of ships) {
      const id = old.ship_id || old.ship;
      if (!ids.includes(id)) fadingShips[id] = fadingShips[id] ?? 1.0;
    }
    ships = newShips;
    document.getElementById("shipcount").textContent = `VESSELS: ${ships.length}`;
  } catch (err) {
    console.error("Bad WS data:", err);
  }
};

ws.onerror = (e) => console.error("WS error:", e);
ws.onclose = () => console.log("🔌 Radar bridge disconnected");