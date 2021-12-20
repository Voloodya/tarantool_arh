#! /usr/bin/env python
# encoding: utf-8
from panflute import Link, run_filter, Para, Str, Header, MetaInlines, Space, debug, Div, HorizontalRule, \
    BulletList, RawBlock, Span, Emph, Strong, Image, Block, Inline, ListContainer


version = None

pb = RawBlock(u"<w:p><w:r><w:br w:type=\"page\" /></w:r></w:p>", format=u"openxml")


def action(elem, doc):
    if isinstance(elem, Image):
        # debug(elem.content)
        if isinstance(elem.content, ListContainer):
            attributes={'custom-style': 'image'}
            # if hasattr(elem.parent, 'attributes'):
            # debug(elem.parent)

        return elem
    if isinstance(elem, MetaInlines):
        """
        Title page header
        """
        global version
        meta = MetaInlines(Str('Tarantool Data Grid (TDG)'))
        return meta

    if isinstance(elem, Para) and isinstance(elem.content[0], Span):
        """
        Image caption
        """
        _span = elem.content[0]
        classes = getattr(_span, 'classes', [])
        if 'caption-text' in classes:
            attrs = {'custom-style': 'image-caption'}
            caption_block = Div(elem, attributes=attrs)
            return caption_block
        return elem

    with_classes = getattr(elem, 'classes', [])

    if 'caption-text' in with_classes:
        elem.attributes = {'custom-style': 'captiontext'}
        return elem

    if 'image' in with_classes:
        elem.attributes = {'custom-style': 'image'}
        return elem

    if 'headerlink' in with_classes:
        return []

    if 'toc-backref' in with_classes:
        """
        Remove headers backrefs
        """
        return Span(*elem.content)

    elif 'local-toc' in with_classes:
        """
        Remove toctree from html template
        """
        return []

    if isinstance(elem, HorizontalRule):
        """
        Remove hr
        """
        return []

    if len(with_classes) and with_classes[0] == 'breadcrumbs_and_search':
        """
        Remove breadcrumbs
        """
        return []

    if len(with_classes) and 'admonition' in with_classes:
        """
        Customize admonition styles
        """
        elem.attributes = {'custom-style': 'admonitionlist'}
        elem.content[0].content[0] = Strong(Str('Примечание'))
        return elem

    if isinstance(elem, Link):
        """
        Transform html urls in docx format
        """
        elem.url = elem.url.replace('index-docx.html', '')
        return elem


def main(doc=None):
    return run_filter(action, doc=doc)


if __name__ == '__main__':
    main()
