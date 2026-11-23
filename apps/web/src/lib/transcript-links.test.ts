import { describe, expect, test } from 'bun:test'
import { transcriptLinkRoute } from './transcript-links'

describe('transcriptLinkRoute', () => {
  const workspace = '/Users/egoist/dev/waku'

  test('opens absolute workspace files in the project editor', () => {
    expect(transcriptLinkRoute(
      '/Users/egoist/dev/waku/src/app/right_panel.rs:1596:8',
      workspace,
    )).toEqual({ kind: 'projectFile', path: 'src/app/right_panel.rs' })
    expect(transcriptLinkRoute(
      'file:///Users/egoist/dev/waku/My%20File.rs#L12C4',
      workspace,
    )).toEqual({ kind: 'projectFile', path: 'My File.rs' })
  })

  test('routes agentos://session and codeg://session links directly to sessions', () => {
    expect(transcriptLinkRoute(
      'agentos://session/123e4567-e89b-12d3-a456-426614174000',
      workspace,
    )).toEqual({ kind: 'session', sessionId: '123e4567-e89b-12d3-a456-426614174000' })
    expect(transcriptLinkRoute(
      'codeg://session/1861',
      workspace,
    )).toEqual({ kind: 'session', sessionId: '1861' })
  })

  test('routes outside-workspace text files to read-only external preview and rejects binary/dir links', () => {
    expect(transcriptLinkRoute(
      '/Users/egoist/dev/waku/../kero/src/app.rs:20',
      workspace,
    )).toEqual({ kind: 'externalFile', path: '/Users/egoist/dev/kero/src/app.rs' })
    expect(transcriptLinkRoute(
      'file:///Volumes/data/brain/nlp_cw1_plan.md#L10-L20',
      workspace,
    )).toEqual({ kind: 'externalFile', path: '/Volumes/data/brain/nlp_cw1_plan.md' })
    expect(transcriptLinkRoute(
      'file:///Volumes/data/brain/archive.zip',
      workspace,
    )).toEqual({ kind: 'remoteFile', path: '/Volumes/data/brain/archive.zip' })
    expect(transcriptLinkRoute(
      'file:///Volumes/data/brain/',
      workspace,
    )).toEqual({ kind: 'remoteFile', path: '/Volumes/data/brain' })
    expect(transcriptLinkRoute('https://example.com/file.rs:12', workspace))
      .toEqual({ kind: 'external' })
    expect(transcriptLinkRoute('src/app.rs', workspace)).toEqual({ kind: 'external' })
  })
})
