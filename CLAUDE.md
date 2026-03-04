# Project Overview

This is a full-stack web application built with Next.js 14 (App Router) and TypeScript. Users can create accounts, manage data through a dashboard, and interact with a backend API. The system handles authentication, data storage, and user interactions through a modern web interface.

The architecture is modular: UI components, business logic, and API functionality are intentionally separated for maintainability and scalability.

---

# Tech Stack

- **Language:** TypeScript (strict typing required everywhere)
- **Framework:** Next.js 14 — App Router only (no Pages Router)
- **Database:** PostgreSQL via Prisma ORM
- **Authentication:** NextAuth.js
- **Styling:** Tailwind CSS
- **State Management:** React Context and/or Zustand
- **Key Libraries:**
  - `axios` — all HTTP requests
  - `zod` — schema validation for all external data
  - `prisma` — database ORM and migrations
  - `next-auth` — authentication sessions and providers
  - `react-hook-form` — all form handling

---

# Project Structure

/src
  /app          → Next.js App Router pages and layouts
  /components   → Reusable UI components (buttons, forms, modals, layout)
  /api          → Backend API route handlers
  /lib          → Shared utilities, helpers, API client config
  /services     → Business logic and service layer
  /hooks        → Custom React hooks
  /types        → TypeScript types and interfaces

/prisma         → Prisma schema and database migrations
/public         → Static assets (images, icons)

---

# Coding Conventions

### General
- TypeScript everywhere — type all functions, components, props, and return values
- Use async/await only — never .then() or .catch() chains
- Use camelCase for variables and functions
- Use PascalCase for React components and TypeScript interfaces/types

### React & Components
- Functional components only — no class components
- Keep components small and single-purpose
- No heavy logic inside components — move it to /src/services or /src/hooks
- Validate form inputs with react-hook-form + zod resolver

### Data & API
- All API requests must go through the central client at /src/lib/api.ts — never use raw axios elsewhere
- Validate all external/untrusted data with Zod schemas before using it
- Define shared types in /src/types — never inline complex types in components

### Business Logic
- Service layer lives in /src/services — UI components should call services, not implement logic
- Keep API route handlers thin — delegate to services for processing

---

# Critical Rules (Never Break These)

- Never modify /prisma/schema.prisma without immediately running: npx prisma migrate dev
- Never commit .env or any file containing secrets or API keys
- Never bypass /src/lib/api.ts for API requests — all calls go through the central client
- Never put business logic directly in React components — use /src/services or /src/hooks
- Never use .then() — always use async/await
- Auth is fully managed by NextAuth.js — do not build custom auth logic

---

# Data Flow

User Interaction (UI Component)
  → Custom Hook (/src/hooks)
    → Service Layer (/src/services)
      → API Client (/src/lib/api.ts)
        → API Route Handler (/src/api)
          → Prisma ORM (/prisma)
            → PostgreSQL Database

Authentication flows through NextAuth.js session management. All incoming data is validated with Zod before entering the service layer.

---

# Grounding Instruction (Run at Start of Every Session)

Before doing anything, read this CLAUDE.md file and the relevant files in /src.
Then summarize:
1. The overall architecture and how data flows through the application
2. Which modules are involved in the task at hand
3. Any constraints or rules that apply to this task

Do not write any code until you have confirmed your understanding.