export type TranscriptLinkRoute =
  | { kind: 'session'; sessionId: string }
  | { kind: 'projectFile'; path: string }
  | { kind: 'externalFile'; path: string }
  | { kind: 'remoteFile'; path: string }
  | { kind: 'external' }

const NON_TEXT_EXTERNAL_EXTENSIONS = new Set([
  'png', 'jpg', 'jpeg', 'gif', 'webp', 'avif', 'ico', 'tiff', 'bmp', 'heic',
  'mp3', 'wav', 'flac', 'ogg', 'm4a',
  'mp4', 'mov', 'avi', 'webm', 'mkv',
  'zip', 'gz', 'tgz', 'bz2', 'xz', '7z', 'rar', 'tar', 'jar', 'dmg', 'pkg', 'app',
  'db', 'sqlite', 'sqlite3', 'exe', 'dll', 'so', 'dylib', 'wasm', 'class', 'pyc', 'o', 'a',
  'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'pages', 'numbers', 'key',
])

export function transcriptLinkRoute(target: string, workspace?: string): TranscriptLinkRoute {
  const trimmed = target.trim()
  const sessionMatch = trimmed.match(/^(?:agentos|codeg):\/\/session\/([^/?#]+)$/i)
  if (sessionMatch?.[1]) {
    return { kind: 'session', sessionId: sessionMatch[1].trim() }
  }
  const path = markdownFilePath(trimmed)
  if (!path) return { kind: 'external' }
  const normalizedPath = normalizePath(path)
  const normalizedWorkspace = workspace ? normalizePath(workspace) : null
  if (normalizedWorkspace) {
    const prefix = normalizedWorkspace === '/' ? '/' : `${normalizedWorkspace}/`
    if (normalizedPath.startsWith(prefix) && normalizedPath !== normalizedWorkspace) {
      return { kind: 'projectFile', path: normalizedPath.slice(prefix.length) }
    }
  }
  if (hasTrailingDirectorySeparator(trimmed) || isNonTextExternalPath(normalizedPath)) {
    return { kind: 'remoteFile', path: normalizedPath }
  }
  return { kind: 'externalFile', path: normalizedPath }
}

function isNonTextExternalPath(path: string) {
  const name = path.split('/').at(-1) ?? ''
  const dot = name.lastIndexOf('.')
  if (dot <= 0 || dot === name.length - 1) return false
  return NON_TEXT_EXTERNAL_EXTENSIONS.has(name.slice(dot + 1).toLowerCase())
}

function hasTrailingDirectorySeparator(target: string) {
  const stripped = stripFileLocation(target.trim())
  const pathPart = stripped.split(/[?#]/, 1)[0] ?? stripped
  return pathPart.endsWith('/') || pathPart.endsWith('\\')
}

function markdownFilePath(target: string) {
  const stripped = stripFileLocation(target.trim())
  let path: string
  if (stripped.startsWith('/')) path = stripped
  else if (stripped.startsWith('file://localhost/')) path = stripped.slice('file://localhost'.length)
  else if (stripped.startsWith('file:///')) path = stripped.slice('file://'.length)
  else if (stripped.startsWith('file:/')) path = stripped.slice('file:'.length)
  else return null
  try {
    return decodeURIComponent(path)
  } catch {
    return path
  }
}

function stripFileLocation(target: string) {
  const fragment = target.match(/^(.*)#L\d+(?:C\d+)?(?:-L\d+(?:C\d+)?)?$/)
  if (fragment) return fragment[1]!
  const lineColumn = target.match(/^(.*):\d+:\d+$/)
  if (lineColumn) return lineColumn[1]!
  const line = target.match(/^(.*):\d+$/)
  return line?.[1] ?? target
}

function normalizePath(path: string) {
  const parts: string[] = []
  for (const part of path.replaceAll('\\', '/').split('/')) {
    if (!part || part === '.') continue
    if (part === '..') parts.pop()
    else parts.push(part)
  }
  return `/${parts.join('/')}`
}
