type MdastNode = {
  type: string
  value?: string
  children?: MdastNode[]
  position?: unknown
}

const HTML_BR_PATTERN = /^<br\s*\/?>$/i

/**
 * Convert standalone `<br>` / `<br/>` / `<br />` HTML nodes into standard
 * mdast `break` nodes without enabling raw HTML rendering (`rehype-raw`),
 * keeping XSS protection intact and preserving source positions for
 * streaming veils.
 */
export function remarkHtmlBr() {
  return (tree: MdastNode) => {
    visit(tree)
  }
}

function visit(node: MdastNode) {
  if (!node.children) return
  for (let index = 0; index < node.children.length; index++) {
    const child = node.children[index]!
    if (
      child.type === 'html'
      && typeof child.value === 'string'
      && HTML_BR_PATTERN.test(child.value.trim())
    ) {
      node.children[index] = child.position !== undefined
        ? { type: 'break', position: child.position }
        : { type: 'break' }
    } else {
      visit(child)
    }
  }
}
