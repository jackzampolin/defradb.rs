# Phase 0: Scope Definition

The most important phase. Get this right and the rest flows naturally.

## How It Works

Human and Claude collaborate to identify 4-8 audit streams based on what the codebase actually does. This is NOT a checklist — it's an analysis of the specific trust boundaries, attack surfaces, and security-critical subsystems in this project.

## Step 1: Codebase Reconnaissance

Before defining streams, Claude does a quick structural survey:

```
- Directory tree (crate/module structure)
- LOC per crate/module
- External dependencies (Cargo.toml / package.json / go.mod)
- Trust boundaries (where does untrusted input enter?)
- Cryptographic usage points
- Network-facing code
- Unsafe code / FFI boundaries
- Storage / persistence layer
- Authentication / authorization code
```

## Step 2: Identify Candidate Streams

Group the codebase into security-relevant areas. Common patterns:

### For a database (like defradb.rs):
- Cryptographic primitives
- Access control / authorization
- P2P networking
- Identity / authentication
- Input validation (query parsing)
- Data integrity (CRDT/merge correctness)
- Dependencies / unsafe code

### For a game engine:
- Asset loading / deserialization
- Networking / multiplayer protocol
- Scripting engine sandbox
- Memory management / unsafe code
- Input handling (controller, keyboard)
- Rendering pipeline (shader injection, buffer overflows)
- Dependencies / build pipeline

### For a web application:
- Authentication / session management
- Authorization / access control
- Input validation / injection
- API surface (REST, GraphQL, WebSocket)
- Cryptographic usage (tokens, encryption)
- File handling / upload
- Dependencies / supply chain

The point: **derive the streams from the codebase, not from a template.**

## Step 3: Stream Definition

For each stream, define:

| Field | Description |
|-------|-------------|
| **Name** | Short, descriptive (e.g., "P2P Network Security") |
| **Scope** | What this stream covers and WHY it matters |
| **Key Questions** | 3-5 security questions this stream must answer |
| **Crates/Modules** | Which code is in scope |
| **Trust Boundaries** | Where untrusted data enters this subsystem |

## Step 4: Create Audit Directory Structure

```bash
mkdir -p audit/{01-stream-name-findings,02-stream-name-findings,...}
```

Each stream gets:
- A plan file: `audit/XX-stream-name.md`
- A findings directory: `audit/XX-stream-name-findings/`

## Step 5: Human Review

The human reviews the proposed streams and adjusts:
- Merge streams that are too small
- Split streams that are too large (>8 sessions)
- Add streams for areas Claude might miss (business logic, compliance)
- Reorder by priority (most security-critical first)

## Output

A set of plan files in `audit/` with scope, key questions, and crate assignments. Ready for Phase 1 (Reconnaissance).

## Time Budget

~30 minutes of human-Claude conversation. Don't rush this — bad scope definition leads to shallow findings or missed areas.
