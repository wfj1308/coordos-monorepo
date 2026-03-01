# Design Institute UI Console (React + Tailwind)

Frontend console for validating CoordOS service behaviors.

Stack:
- React 18
- TailwindCSS
- Vite

## Install

```bash
cd ui/design-institute
npm install
```

## Run (dev)

```bash
npm run dev
```

Open: `http://127.0.0.1:5173`

## Build

```bash
npm run build
npm run preview
```

## Usage

1. Start backend services:
   - `go run ./services/design-institute`
   - `go run ./services/vault-service`
2. Open UI in browser.
3. Run the built-in scenario:
   - `中北桥梁项目核心主流程闭环`
   - Covers: project -> contract -> employee -> qualification -> achievement -> invoice -> settlement -> project resources.
4. Or use quick templates / custom request console for ad-hoc API checks.

## Files

- `src/App.jsx`: page and request console.
- `src/index.css`: Tailwind entry and shared component styles.
- `tailwind.config.js`: theme and scanning config.
