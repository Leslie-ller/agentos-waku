import { describe, expect, test } from 'bun:test'
import {
  normalizeDaemonAddress,
  parsePairingHash,
  validateConnectionConfig,
} from './connection'

describe('normalizeDaemonAddress', () => {
  test('normalizes host, HTTP, and daemon paths', () => {
    expect(normalizeDaemonAddress('host.example:34123')).toBe(
      'ws://host.example:34123',
    )
    expect(normalizeDaemonAddress('https://waku.example/v1?token=nope')).toBe(
      'wss://waku.example',
    )
    expect(normalizeDaemonAddress('HTTP://WAKU.EXAMPLE/v1')).toBe(
      'ws://waku.example',
    )
  })

  test('rejects unsupported schemes and credentials', () => {
    expect(() => normalizeDaemonAddress('ftp://waku.example')).toThrow()
    expect(() => normalizeDaemonAddress('ws://token@waku.example')).toThrow()
  })

  test('requires a token without putting it in the address', () => {
    expect(() =>
      validateConnectionConfig({ address: 'waku.example', token: '  ' }),
    ).toThrow('token')
    expect(
      validateConnectionConfig({ address: 'waku.example', token: 'secret' }),
    ).toEqual({
      address: 'ws://waku.example',
      token: 'secret',
      remember: false,
    })
  })

  test('parses one-tap mobile/Tailscale pairing hashes using same-origin fallback', () => {
    expect(
      parsePairingHash('#token=tailscale-secret', 'http://100.88.12.34:34123'),
    ).toEqual({
      address: 'ws://100.88.12.34:34123',
      token: 'tailscale-secret',
      remember: true,
    })
    expect(
      parsePairingHash(
        '#token=ephemeral&address=https%3A%2F%2Fwaku.ts.net&remember=0',
        'http://localhost:34123',
      ),
    ).toEqual({
      address: 'wss://waku.ts.net',
      token: 'ephemeral',
      remember: false,
    })
    expect(parsePairingHash('#session=123', 'http://localhost:34123')).toBeNull()
  })
})
