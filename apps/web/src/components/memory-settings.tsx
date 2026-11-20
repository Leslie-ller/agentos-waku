import { useQueryClient } from '@tanstack/react-query'
import type {
  MemoryCheckpointRecord,
  MemoryEntryKind,
  MemoryKnowledgeRecord,
  MemoryNoteRecord,
} from '@waku/client'
import { useMemo, useState } from 'react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { WakuIcon } from '@/components/waku-icon'
import { useMemoryLibrary } from '@/hooks/use-daemon-data'
import {
  daemonKeys,
  deleteMemoryEntry,
  updateMemoryKnowledgeStatus,
} from '@/lib/daemon-api'
import { useDaemon } from '@/lib/daemon-context'
import { useI18n } from '@/lib/i18n'
import { cn } from '@/lib/utils'

type MemoryTab = 'knowledge' | 'notes' | 'checkpoints'
type MemoryScopeFilter = 'current' | 'all'

export function MemorySettings({
  currentProjectPath,
}: {
  currentProjectPath?: string | null
}) {
  const { t } = useI18n()
  const { client, config } = useDaemon()
  const queryClient = useQueryClient()
  const memory = useMemoryLibrary()
  const [tab, setTab] = useState<MemoryTab>('knowledge')
  const [scope, setScope] = useState<MemoryScopeFilter>(
    currentProjectPath ? 'current' : 'all',
  )
  const [query, setQuery] = useState('')
  const [busyKey, setBusyKey] = useState<string | null>(null)

  const normalizedQuery = query.trim().toLowerCase()
  const normalizedProject = currentProjectPath?.trim() || null

  const filteredKnowledge = useMemo(() => {
    const list = memory.data?.knowledge ?? []
    return list.filter((item) => {
      if (scope === 'current' && normalizedProject && item.projectPath !== normalizedProject) {
        return false
      }
      if (!normalizedQuery) return true
      const haystack = `${item.claim} ${item.projectPath} ${item.sessionTitle ?? ''} ${item.scope} ${item.status}`.toLowerCase()
      return haystack.includes(normalizedQuery)
    })
  }, [memory.data?.knowledge, normalizedProject, normalizedQuery, scope])

  const filteredNotes = useMemo(() => {
    const list = memory.data?.notes ?? []
    return list.filter((item) => {
      if (scope === 'current' && normalizedProject && item.projectPath !== normalizedProject) {
        return false
      }
      if (!normalizedQuery) return true
      const haystack = `${item.noteId} ${item.body} ${item.projectPath} ${item.sessionTitle ?? ''} ${item.scope}`.toLowerCase()
      return haystack.includes(normalizedQuery)
    })
  }, [memory.data?.notes, normalizedProject, normalizedQuery, scope])

  const filteredCheckpoints = useMemo(() => {
    const list = memory.data?.checkpoints ?? []
    return list.filter((item) => {
      if (scope === 'current' && normalizedProject && item.projectPath !== normalizedProject) {
        return false
      }
      if (!normalizedQuery) return true
      const haystack = `${item.checkpointId} ${item.payloadJson} ${item.projectPath} ${item.sessionTitle ?? ''}`.toLowerCase()
      return haystack.includes(normalizedQuery)
    })
  }, [memory.data?.checkpoints, normalizedProject, normalizedQuery, scope])

  async function refresh() {
    await memory.refetch()
  }

  async function setKnowledgeStatus(knowledgeId: string, status: 'promoted' | 'rejected') {
    if (!client || !config) return
    const key = `status:${knowledgeId}`
    setBusyKey(key)
    try {
      await updateMemoryKnowledgeStatus(client, knowledgeId, status)
      await queryClient.invalidateQueries({
        queryKey: daemonKeys.memoryLibrary(config.address),
      })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : String(error))
    } finally {
      setBusyKey(null)
    }
  }

  async function removeEntry(
    kind: MemoryEntryKind,
    id: string,
    noteId?: string | null,
    entryScope?: string | null,
  ) {
    if (!client || !config) return
    const key = `delete:${kind}:${id}:${noteId ?? ''}`
    setBusyKey(key)
    try {
      await deleteMemoryEntry(client, kind, id, noteId, entryScope)
      await queryClient.invalidateQueries({
        queryKey: daemonKeys.memoryLibrary(config.address),
      })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : String(error))
    } finally {
      setBusyKey(null)
    }
  }

  return (
    <div className="mt-3 space-y-4">
      <p className="text-[12.5px] leading-[18px] text-[var(--text-secondary)]">
        {t('memory.subtitle')}
      </p>

      <div className="flex flex-col gap-2.5 sm:flex-row sm:items-center">
        <label className="flex h-9 min-w-0 flex-1 items-center gap-2 rounded-lg border bg-[var(--inset)] px-2.5 focus-within:border-ring">
          <WakuIcon className="size-3.5 shrink-0 text-[var(--text-tertiary)]" name="search" />
          <input
            aria-label={t('memory.search')}
            className="min-w-0 flex-1 bg-transparent text-[12.5px] outline-none placeholder:text-[var(--text-ghost)]"
            placeholder={t('memory.search')}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
        </label>
        <div className="flex flex-wrap items-center gap-1.5">
          {normalizedProject && (
            <div className="flex rounded-lg border bg-[var(--inset)] p-0.5 text-[11.5px]">
              <button
                className={cn(
                  'rounded-md px-2.5 py-1 transition-colors',
                  scope === 'current'
                    ? 'bg-background font-medium text-foreground shadow-xs'
                    : 'text-[var(--text-secondary)] hover:text-foreground',
                )}
                type="button"
                onClick={() => setScope('current')}
              >
                {t('memory.current_project')}
              </button>
              <button
                className={cn(
                  'rounded-md px-2.5 py-1 transition-colors',
                  scope === 'all'
                    ? 'bg-background font-medium text-foreground shadow-xs'
                    : 'text-[var(--text-secondary)] hover:text-foreground',
                )}
                type="button"
                onClick={() => setScope('all')}
              >
                {t('memory.all_projects')}
              </button>
            </div>
          )}
          <Button
            disabled={memory.isFetching}
            size="sm"
            variant="outline"
            onClick={() => void refresh()}
          >
            <WakuIcon
              className={cn('size-3.5', memory.isFetching && 'motion-safe:animate-spin')}
              name="rotateCw"
            />
            {t(memory.isFetching ? 'memory.refreshing' : 'memory.refresh')}
          </Button>
        </div>
      </div>

      <div className="flex flex-wrap gap-1.5 border-b pb-2.5">
        {([
          { id: 'knowledge', label: t('memory.tab_knowledge'), count: filteredKnowledge.length },
          { id: 'notes', label: t('memory.tab_notes'), count: filteredNotes.length },
          { id: 'checkpoints', label: t('memory.tab_checkpoints'), count: filteredCheckpoints.length },
        ] as const).map((item) => (
          <button
            className={cn(
              'flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[12.5px] transition-colors',
              tab === item.id
                ? 'bg-accent font-medium text-foreground'
                : 'text-[var(--text-secondary)] hover:bg-accent/60 hover:text-foreground',
            )}
            key={item.id}
            type="button"
            onClick={() => setTab(item.id)}
          >
            <span>{item.label}</span>
            <span className="rounded-full bg-[var(--inset)] px-1.5 py-0.5 text-[10.5px] tabular-nums text-[var(--text-tertiary)]">
              {item.count}
            </span>
          </button>
        ))}
      </div>

      {memory.isPending && (
        <div className="rounded-[13px] bg-[var(--raised)] px-5 py-8 text-center text-[12.5px] text-[var(--text-secondary)]">
          {t('memory.loading')}
        </div>
      )}

      {memory.error && (
        <div className="rounded-[13px] bg-[var(--danger-soft)] px-4 py-3 text-[12.5px] text-destructive">
          {memory.error instanceof Error ? memory.error.message : String(memory.error)}
        </div>
      )}

      {!memory.isPending && tab === 'knowledge' && (
        filteredKnowledge.length === 0 ? (
          <EmptyMemoryState label={t('memory.empty_knowledge')} />
        ) : (
          <div className="space-y-2.5">
            {filteredKnowledge.map((item) => (
              <KnowledgeCard
                busy={busyKey === `status:${item.knowledgeId}` || busyKey === `delete:knowledge:${item.knowledgeId}:`}
                item={item}
                key={item.knowledgeId}
                onDelete={() => void removeEntry('knowledge', item.knowledgeId)}
                onPromote={() => void setKnowledgeStatus(item.knowledgeId, 'promoted')}
                onReject={() => void setKnowledgeStatus(item.knowledgeId, 'rejected')}
              />
            ))}
          </div>
        )
      )}

      {!memory.isPending && tab === 'notes' && (
        filteredNotes.length === 0 ? (
          <EmptyMemoryState label={t('memory.empty_notes')} />
        ) : (
          <div className="space-y-2.5">
            {filteredNotes.map((item) => (
              <NoteCard
                busy={busyKey === `delete:note:${item.sessionId}:${item.noteId}`}
                item={item}
                key={`${item.sessionId}:${item.noteId}:${item.scope}`}
                onDelete={() => void removeEntry('note', item.sessionId, item.noteId, item.scope)}
              />
            ))}
          </div>
        )
      )}

      {!memory.isPending && tab === 'checkpoints' && (
        filteredCheckpoints.length === 0 ? (
          <EmptyMemoryState label={t('memory.empty_checkpoints')} />
        ) : (
          <div className="space-y-2.5">
            {filteredCheckpoints.map((item) => (
              <CheckpointCard
                busy={busyKey === `delete:checkpoint:${item.checkpointId}:`}
                item={item}
                key={item.checkpointId}
                onDelete={() => void removeEntry('checkpoint', item.checkpointId)}
              />
            ))}
          </div>
        )
      )}
    </div>
  )
}

function EmptyMemoryState({ label }: { label: string }) {
  return (
    <div className="rounded-[13px] bg-[var(--raised)] px-5 py-8 text-center text-[12.5px] text-[var(--text-tertiary)]">
      {label}
    </div>
  )
}

function KnowledgeCard({
  item,
  busy,
  onPromote,
  onReject,
  onDelete,
}: {
  item: MemoryKnowledgeRecord
  busy: boolean
  onPromote: () => void
  onReject: () => void
  onDelete: () => void
}) {
  const { t } = useI18n()
  const statusLabel = item.status === 'promoted'
    ? t('memory.status_promoted')
    : item.status === 'rejected'
      ? t('memory.status_rejected')
      : t('memory.status_candidate')
  return (
    <article className="rounded-[13px] bg-[var(--raised)] px-4 py-3.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-[var(--text-tertiary)]">
        <span
          className={cn(
            'rounded-full px-2 py-0.5 font-medium',
            item.status === 'promoted'
              ? 'bg-[var(--success)]/15 text-[var(--success)]'
              : item.status === 'rejected'
                ? 'bg-[var(--danger-soft)] text-destructive'
                : 'bg-[var(--inset)] text-[var(--text-secondary)]',
          )}
        >
          {statusLabel}
        </span>
        <span className="rounded-md bg-[var(--inset)] px-1.5 py-0.5">{item.scope}</span>
        <span className="truncate">{projectBasename(item.projectPath)}</span>
        {item.sessionTitle && (
          <span className="max-w-[200px] truncate text-[var(--text-ghost)]">
            · {item.sessionTitle}
          </span>
        )}
        <span className="ml-auto tabular-nums">{formatTimestamp(item.createdAt)}</span>
      </div>
      <p className="mt-2 whitespace-pre-wrap break-words text-[13px] leading-[19px] text-foreground">
        {item.claim}
      </p>
      <div className="mt-3 flex flex-wrap justify-end gap-1.5">
        {item.status !== 'promoted' && (
          <Button disabled={busy} size="sm" variant="outline" onClick={onPromote}>
            {t('memory.action_promote')}
          </Button>
        )}
        {item.status !== 'rejected' && (
          <Button disabled={busy} size="sm" variant="ghost" onClick={onReject}>
            {t('memory.action_reject')}
          </Button>
        )}
        <Button disabled={busy} size="sm" variant="ghost" onClick={onDelete}>
          {t('memory.action_delete')}
        </Button>
      </div>
    </article>
  )
}

function NoteCard({
  item,
  busy,
  onDelete,
}: {
  item: MemoryNoteRecord
  busy: boolean
  onDelete: () => void
}) {
  const { t } = useI18n()
  return (
    <article className="rounded-[13px] bg-[var(--raised)] px-4 py-3.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-[var(--text-tertiary)]">
        <span className="font-mono font-medium text-foreground">{item.noteId}</span>
        <span className="rounded-md bg-[var(--inset)] px-1.5 py-0.5">r{item.revision}</span>
        <span className="rounded-md bg-[var(--inset)] px-1.5 py-0.5">{item.scope}</span>
        <span className="truncate">{projectBasename(item.projectPath)}</span>
        {item.sessionTitle && (
          <span className="max-w-[200px] truncate text-[var(--text-ghost)]">
            · {item.sessionTitle}
          </span>
        )}
        <span className="ml-auto tabular-nums">{formatTimestamp(item.updatedAt)}</span>
      </div>
      <pre className="mt-2 max-h-56 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-background/60 p-2.5 font-sans text-[12.5px] leading-[18px] text-[var(--text-secondary)]">
        {item.body}
      </pre>
      <div className="mt-2.5 flex justify-end">
        <Button disabled={busy} size="sm" variant="ghost" onClick={onDelete}>
          {t('memory.action_delete')}
        </Button>
      </div>
    </article>
  )
}

function CheckpointCard({
  item,
  busy,
  onDelete,
}: {
  item: MemoryCheckpointRecord
  busy: boolean
  onDelete: () => void
}) {
  const { t } = useI18n()
  const sections = parseCheckpointSections(item.payloadJson, t)
  return (
    <article className="rounded-[13px] bg-[var(--raised)] px-4 py-3.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-[var(--text-tertiary)]">
        <span className="font-mono font-medium text-foreground">
          {item.checkpointId.slice(0, 12)}
        </span>
        <span className="truncate">{projectBasename(item.projectPath)}</span>
        {item.sessionTitle && (
          <span className="max-w-[200px] truncate text-[var(--text-ghost)]">
            · {item.sessionTitle}
          </span>
        )}
        <span className="ml-auto tabular-nums">{formatTimestamp(item.createdAt)}</span>
      </div>
      {sections.length > 0 ? (
        <div className="mt-2.5 space-y-2">
          {sections.map((section) => (
            <div key={section.label}>
              <div className="text-[11px] font-medium text-[var(--text-tertiary)]">
                {section.label}
              </div>
              <ul className="mt-1 list-disc space-y-0.5 pl-4 text-[12.5px] leading-[18px] text-foreground">
                {section.items.map((text, idx) => (
                  <li className="break-words" key={idx}>{text}</li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      ) : (
        <pre className="mt-2 max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-background/60 p-2.5 font-mono text-[11.5px] text-[var(--text-secondary)]">
          {item.payloadJson}
        </pre>
      )}
      <div className="mt-2.5 flex justify-end">
        <Button disabled={busy} size="sm" variant="ghost" onClick={onDelete}>
          {t('memory.action_delete')}
        </Button>
      </div>
    </article>
  )
}

function parseCheckpointSections(
  payloadJson: string,
  t: (key: string) => string,
): Array<{ label: string; items: string[] }> {
  try {
    const parsed = JSON.parse(payloadJson) as Record<string, unknown>
    const mapping: Array<[string, string]> = [
      ['completed', t('memory.cp_completed')],
      ['in_progress', t('memory.cp_in_progress')],
      ['next_actions', t('memory.cp_next_actions')],
      ['open_questions', t('memory.cp_open_questions')],
      ['constraints', t('memory.cp_constraints')],
      ['rejected_approaches', t('memory.cp_rejected')],
    ]
    const sections: Array<{ label: string; items: string[] }> = []
    for (const [key, label] of mapping) {
      const value = parsed[key]
      if (!Array.isArray(value)) continue
      const items = value
        .filter((entry): entry is string => typeof entry === 'string')
        .map((entry) => entry.trim())
        .filter(Boolean)
      if (items.length) sections.push({ label, items })
    }
    return sections
  } catch {
    return []
  }
}

function projectBasename(path: string): string {
  const cleaned = path.replace(/[\\/]+$/, '')
  const parts = cleaned.split(/[\\/]/)
  return parts.at(-1) || path
}

function formatTimestamp(ts: number): string {
  if (!Number.isFinite(ts) || ts <= 0) return ''
  const date = new Date(ts > 1e12 ? ts : ts * 1_000)
  return date.toLocaleString(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}
