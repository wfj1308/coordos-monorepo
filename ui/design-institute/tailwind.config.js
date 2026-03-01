/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'Noto Sans SC'", "'IBM Plex Sans'", "system-ui", "sans-serif"],
      },
      colors: {
        ink: "#0f172a",
        slate: "#334155",
        mist: "#e2e8f0",
        skyline: "#0ea5e9",
        mint: "#10b981",
        amber: "#f59e0b",
      },
      boxShadow: {
        panel: "0 10px 30px rgba(15, 23, 42, 0.14)",
      },
      backgroundImage: {
        paper:
          "radial-gradient(circle at 12% 18%, rgba(14,165,233,0.14), transparent 36%), radial-gradient(circle at 80% 12%, rgba(16,185,129,0.12), transparent 38%), linear-gradient(135deg, #f8fafc 0%, #ecfeff 56%, #f0f9ff 100%)",
      },
    },
  },
  plugins: [],
};
