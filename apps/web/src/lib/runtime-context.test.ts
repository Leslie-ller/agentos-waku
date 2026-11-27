import { describe, expect, test } from 'bun:test'
import type { AgentSession } from '@waku/client'
import { beginTurn, createSession } from './daemon-api'
import { sessionRuntimeCanClose, type BackgroundWorkItem } from './runtime-context'

function makeBackgroundItem(status: BackgroundWorkItem['status']): BackgroundWorkItem {
  return {
    key: { kind: 'process', providerId: 'proc-1' },
    title: 'sleep 30',
    detail: null,
    command: 'sleep 30',
    cwd: '/tmp',
    output: null,
    outputTruncated: false,
    startedAtMs: 1,
    updatedAtMs: 1,
    durationMs: null,
    exitCode: null,
    background: true,
    canStop: true,
    controlId: 'proc-1',
    originActivityId: null,
    role: null,
    model: null,
    parentId: null,
    status,
  }
}

describe('sessionRuntimeCanClose (AGE-187)', () => {
  test('defers runtime close while turn or background work is active', () => {
    const base = createSession('proj-1', 'codex', false)
    const working: AgentSession = {
      ...beginTurn(base, 'work'),
      status: 'working',
    }

    expect(sessionRuntimeCanClose(working, [])).toBe(false)
    expect(sessionRuntimeCanClose({ ...working, status: 'background' }, [makeBackgroundItem('running')])).toBe(false)
    expect(sessionRuntimeCanClose({ ...working, status: 'idle' }, [])).toBe(false)

    const settled: AgentSession = {
      ...working,
      status: 'idle',
      turns: [{ ...working.turns[0]!, status: 'failed' }],
    }
    expect(sessionRuntimeCanClose(settled, [makeBackgroundItem('running')])).toBe(false)
    expect(sessionRuntimeCanClose(settled, [makeBackgroundItem('completed')])).toBe(true)
    expect(sessionRuntimeCanClose({ ...settled, status: 'failed' }, [])).toBe(true)
  })
})
