/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: ['selector', '[data-theme="dark"]'],
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: 'var(--surface)',
          light: '#ffffff',
          dark: '#141414',
        },
        surface2: {
          DEFAULT: 'var(--surface2)',
          light: '#fafafa',
          dark: '#1a1a1a',
        },
        muted: {
          DEFAULT: 'var(--muted)',
          light: '#737373',
          dark: '#a3a3a3',
        },
        accent: {
          DEFAULT: 'var(--accent)',
          bright: 'var(--accent-bright)',
        },
      },
      maxWidth: {
        container: '1160px',
      },
      borderRadius: {
        btn: '24px',
      },
      minHeight: {
        btn: '48px',
      },
    },
  },
  plugins: [],
}
