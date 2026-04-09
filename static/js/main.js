/**
 * main.js - Lógica Modular para la Defensa
 */

// --- 1. CONFIGURACIÓN Y ESTADO ---
const CONFIG = {
    modes: ["sequential", "thread", "process"],
    colors: { sequential: "#d97706", thread: "#0f766e", process: "#1d4ed8", efficiency: "#7c3aed" }
};

const state = { sequential: null, thread: null, process: null };

// --- 2. GESTIÓN DE LA SIMULACIÓN ---
const SimVisualizer = {
    container: document.getElementById('thread-canvas-container'),

    setup(numThreads) {
        this.container.innerHTML = '';
        for (let i = 0; i < numThreads; i++) {
            const core = document.createElement('div');
            core.className = 'core-node';
            core.innerText = `C${i+1}`;
            this.container.appendChild(core);

            const line = document.createElement('div');
            line.className = 'thread-line';
            
            setTimeout(() => {
                const coreRect = core.getBoundingClientRect();
                const contRect = this.container.getBoundingClientRect();
                line.style.top = `${(coreRect.top - contRect.top) + (coreRect.height/2) - 1}px`;
                this.container.appendChild(line);
            }, 0);
        }
    },

    animate(mode, numThreads) {
        const cores = this.container.querySelectorAll('.core-node');
        if (!cores.length) return;

        const particleCount = mode === 'sequential' ? 10 : 8 * numThreads;
        
        for (let i = 0; i < particleCount; i++) {
            const p = document.createElement('div');
            p.className = 'data-particle';
            this.container.appendChild(p);

            const targetIdx = mode === 'sequential' ? 0 : Math.floor(Math.random() * numThreads);
            const targetCore = cores[targetIdx];
            const contRect = this.container.getBoundingClientRect();
            const coreRect = targetCore.getBoundingClientRect();

            p.style.top = `${(coreRect.top - contRect.top) + (coreRect.height/2) - 5}px`;
            p.style.left = `${coreRect.right - contRect.left}px`;

            // Efecto de brillo en core
            setTimeout(() => {
                targetCore.classList.add('active');
                setTimeout(() => targetCore.classList.remove('active'), 300);
            }, i * (mode === 'sequential' ? 180 : 45));

            anime({
                targets: p,
                left: this.container.offsetWidth - 30,
                opacity: [1, 0],
                duration: mode === 'sequential' ? 1500 : 800,
                delay: i * (mode === 'sequential' ? 200 : 50),
                easing: 'easeOutQuart',
                complete: () => p.remove()
            });
        }
    }
};

// --- 3. GESTIÓN DE GRÁFICAS ---
const ChartManager = {
    instances: {},

    init() {
        const commonOptions = { responsive: true, maintainAspectRatio: false, plugins: { legend: false } };
        
        this.instances.time = new Chart(document.getElementById("time-chart"), this.getConfig("Tiempo (s)", CONFIG.colors.sequential, commonOptions));
        this.instances.speedup = new Chart(document.getElementById("speedup-chart"), this.getConfig("Speedup", CONFIG.colors.thread, commonOptions));
        this.instances.cpu = new Chart(document.getElementById("cpu-chart"), this.getConfig("CPU %", CONFIG.colors.process, commonOptions));
        this.instances.efficiency = new Chart(document.getElementById("efficiency-chart"), this.getConfig("Eficiencia", CONFIG.colors.efficiency, commonOptions));
    },

    getConfig(label, color, options) {
        return {
            type: 'bar',
            data: { labels: ["Single", "Thread", "Core"], datasets: [{ label, data: [null, null, null], backgroundColor: color, borderRadius: 8 }] },
            options: options
        };
    },

    update() {
        const hasSequential = !!state.sequential;
        this.instances.time.data.datasets[0].data = [state.sequential?.time_seconds ?? null, state.thread?.time_seconds ?? null, state.process?.time_seconds ?? null];
        this.instances.speedup.data.datasets[0].data = [hasSequential ? 1 : null, state.thread?.speedup ?? null, state.process?.speedup ?? null];
        this.instances.cpu.data.datasets[0].data = [state.sequential?.cpu_percent ?? null, state.thread?.cpu_percent ?? null, state.process?.cpu_percent ?? null];
        this.instances.efficiency.data.datasets[0].data = [hasSequential ? 1 : null, state.thread?.efficiency ?? null, state.process?.efficiency ?? null];
        Object.values(this.instances).forEach(chart => chart.update());
    }
};

// --- 4. CONTROLADOR PRINCIPAL ---
const App = {
    eventSource: null,

    init() {
        ChartManager.init();
        this.bindEvents();
        this.initTheme();
        this.resetAnalyzer(false);
    },

    bindEvents() {
        document.getElementById("benchmark-form").addEventListener("submit", (e) => {
            e.preventDefault();
            this.runExperiment(new FormData(e.currentTarget));
        });

        document.getElementById("reset-btn").addEventListener("click", () => this.resetAnalyzer());

        document.getElementById('theme-toggle').addEventListener('click', () => this.toggleTheme());
    },

    runExperiment(formData) {
        this.closeStream();
        CONFIG.modes.forEach(m => state[m] = null);
        SimVisualizer.setup(parseInt(formData.get('workers')));
        document.getElementById("status-step").textContent = "0/3";
        document.getElementById("status-current").textContent = "Ejecutando...";
        
        const source = new EventSource(`/benchmark-stream?${new URLSearchParams(formData)}`);
        this.eventSource = source;
        let completed = 0;

        source.addEventListener("result", (e) => {
            const data = JSON.parse(e.data);
            state[data.mode] = data;
            this.updateUI(data);
            completed++;
            document.getElementById("status-step").textContent = `${completed}/3`;
            document.getElementById("status-current").textContent = data.label;
        });

        source.addEventListener("done", () => {
            source.close();
            this.eventSource = null;
        });
    },

    closeStream() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
    },

    resetAnalyzer(resetForm = true) {
        this.closeStream();

        if (resetForm) {
            document.getElementById("benchmark-form").reset();
        }

        CONFIG.modes.forEach(m => state[m] = null);
        document.getElementById("status-current").textContent = "Listo";
        document.getElementById("status-step").textContent = "0/3";

        document.querySelectorAll("#metrics-table-body .metric").forEach((cell) => {
            cell.textContent = "--";
        });
        document.getElementById("thread-workers").textContent = "--";
        document.getElementById("process-workers").textContent = "--";

        SimVisualizer.container.innerHTML = "";
        ChartManager.update();
    },

    updateUI(data) {
        const row = document.querySelector(`[data-row="${data.mode}"]`);
        const cells = row.querySelectorAll(".metric");
        cells[0].textContent = data.time_seconds.toFixed(4);
        cells[1].textContent = data.speedup.toFixed(3);
        cells[2].textContent = data.cpu_percent.toFixed(2);
        cells[3].textContent = data.efficiency.toFixed(3);

        if (data.mode !== "sequential") {
            document.getElementById(`${data.mode}-workers`).textContent = data.workers;
        }

        ChartManager.update();
        SimVisualizer.animate(data.mode, data.workers);
    },

    initTheme() {
        const theme = localStorage.getItem('theme') || 'light';
        document.documentElement.setAttribute('data-theme', theme);
        document.getElementById('theme-icon').innerText = theme === 'dark' ? '☀️' : '🌙';
    },

    toggleTheme() {
        const current = document.documentElement.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        document.getElementById('theme-icon').innerText = next === 'dark' ? '☀️' : '🌙';
        localStorage.setItem('theme', next);
    }
};

// Arrancar App
document.addEventListener('DOMContentLoaded', () => App.init());