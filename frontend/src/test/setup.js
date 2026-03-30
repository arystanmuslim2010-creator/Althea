import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

function buildStorage() {
  const state = new Map()
  return {
    getItem: (key) => (state.has(String(key)) ? state.get(String(key)) : null),
    setItem: (key, value) => state.set(String(key), String(value)),
    removeItem: (key) => state.delete(String(key)),
    clear: () => state.clear(),
  }
}

if (
  !globalThis.localStorage ||
  typeof globalThis.localStorage.getItem !== 'function' ||
  typeof globalThis.localStorage.setItem !== 'function'
) {
  globalThis.localStorage = buildStorage()
}

afterEach(() => {
  cleanup()
  if (globalThis.localStorage && typeof globalThis.localStorage.clear === 'function') {
    globalThis.localStorage.clear()
  }
})
