import { createContext, useContext, useEffect, useState } from 'react'

export const DEFAULT_ANALYST_CAPACITY = 50
export const MIN_ANALYST_CAPACITY = 1
export const MAX_ANALYST_CAPACITY = 500

const STORAGE_KEY = 'althea.analystCapacity'

const AnalystCapacityContext = createContext(null)

function clampCapacity(value) {
  if (value === null || value === undefined || value === '') {
    return DEFAULT_ANALYST_CAPACITY
  }
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return DEFAULT_ANALYST_CAPACITY
  return Math.min(MAX_ANALYST_CAPACITY, Math.max(MIN_ANALYST_CAPACITY, Math.round(numeric)))
}

function readStoredCapacity() {
  if (typeof window === 'undefined' || !window.localStorage) {
    return DEFAULT_ANALYST_CAPACITY
  }
  return clampCapacity(window.localStorage.getItem(STORAGE_KEY))
}

export function AnalystCapacityProvider({ children }) {
  const [capacity, setCapacityState] = useState(readStoredCapacity)

  useEffect(() => {
    if (typeof window === 'undefined' || !window.localStorage) return
    window.localStorage.setItem(STORAGE_KEY, String(capacity))
  }, [capacity])

  useEffect(() => {
    if (typeof window === 'undefined') return undefined

    const handleStorage = (event) => {
      if (event.key !== STORAGE_KEY) return
      setCapacityState(clampCapacity(event.newValue))
    }

    window.addEventListener('storage', handleStorage)
    return () => window.removeEventListener('storage', handleStorage)
  }, [])

  const setCapacity = (nextValue) => {
    setCapacityState((currentValue) => {
      const resolvedValue = typeof nextValue === 'function' ? nextValue(currentValue) : nextValue
      return clampCapacity(resolvedValue)
    })
  }

  return (
    <AnalystCapacityContext.Provider
      value={{
        capacity,
        setCapacity,
        minCapacity: MIN_ANALYST_CAPACITY,
        maxCapacity: MAX_ANALYST_CAPACITY,
      }}
    >
      {children}
    </AnalystCapacityContext.Provider>
  )
}

export function useAnalystCapacity() {
  const context = useContext(AnalystCapacityContext)
  if (!context) {
    throw new Error('useAnalystCapacity must be used within AnalystCapacityProvider')
  }
  return context
}
