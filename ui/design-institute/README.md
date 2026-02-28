# Design Institute UI Console

Static frontend console for validating CoordOS service behaviors.

## Files

- `index.html`: layout and interaction entry.
- `styles.css`: visual system (responsive, animated, atmosphere background).
- `app.js`: environment config, quick actions, request composer, response/log rendering.

## Usage

1. Start backend services:
   - `go run ./services/design-institute`
   - `go run ./services/vault-service`
2. Open `ui/design-institute/index.html` in browser.
3. Set base URLs and token, then run quick actions or custom requests.

## Notes

- This is intentionally build-tool-free for fast local validation.
- Config is stored in `localStorage` to persist across refreshes.
