import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { AnalystCapacityProvider, useAnalystCapacity } from './AnalystCapacityContext'

function CapacityProbe() {
  const { capacity, setCapacity } = useAnalystCapacity()
  return (
    <div>
      <span data-testid="capacity">{capacity}</span>
      <button type="button" onClick={() => setCapacity(100)}>Set Capacity</button>
    </div>
  )
}

describe('AnalystCapacityContext', () => {
  it('persists capacity changes and shares them across consumers', () => {
    render(
      <AnalystCapacityProvider>
        <CapacityProbe />
        <CapacityProbe />
      </AnalystCapacityProvider>,
    )

    expect(screen.getAllByTestId('capacity').map((item) => item.textContent)).toEqual(['50', '50'])

    fireEvent.click(screen.getAllByText('Set Capacity')[0])

    expect(screen.getAllByTestId('capacity').map((item) => item.textContent)).toEqual(['100', '100'])
    expect(globalThis.localStorage.getItem('althea.analystCapacity')).toBe('100')
  })
})
