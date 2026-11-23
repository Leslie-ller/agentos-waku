import { describe, expect, test } from 'bun:test'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { remarkHtmlBr } from './markdown-br'
import { markdownVeilPlugin } from './markdown-veil'

function render(markdown: string, chunks: Parameters<typeof markdownVeilPlugin>[0] = []) {
  return renderToStaticMarkup(
    createElement(
      ReactMarkdown,
      {
        remarkPlugins: [remarkGfm, remarkHtmlBr],
        rehypePlugins: chunks.length ? [markdownVeilPlugin(chunks, 100)] : [],
      },
      markdown,
    ),
  )
}

describe('remarkHtmlBr', () => {
  test('renders <br>, <br/>, and <BR /> as real line breaks in paragraphs and GFM tables', () => {
    expect(render('first<br>second<br/>third<BR />fourth')).toBe(
      '<p>first<br/>\nsecond<br/>\nthird<br/>\nfourth</p>',
    )

    const table = render('| Col A | Col B |\n| --- | --- |\n| line 1<br>line 2 | ok |')
    expect(table).toContain('<td>line 1<br/>\nline 2</td>')
    expect(table).not.toContain('&lt;br&gt;')
  })

  test('leaves inline code, fenced code, partial tags, and non-br HTML safe', () => {
    expect(render('code `<br>` and partial <br')).toBe(
      '<p>code <code>&lt;br&gt;</code> and partial &lt;br</p>',
    )
    expect(render('```html\n<br>\n```')).toContain('&lt;br&gt;')

    const unsafeHtml = render('before <br class="x"> <script>alert(1)</script> after')
    expect(unsafeHtml).not.toContain('<script>')
    expect(unsafeHtml).not.toContain('class="x"')
  })

  test('preserves source offsets for streaming markdownVeilPlugin', () => {
    const markdown = 'hello<br>world'
    const html = render(markdown, [
      { start: 9, end: 14, startedAt: 100, durationMs: 400 },
    ])
    expect(html).toContain('hello<br/>\n<span class="markdown-stream-veil"')
    expect(html).toContain('>world</span>')
  })
})
