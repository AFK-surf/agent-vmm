import AppKit
import SwiftUI

struct TerminalOutputTextView: NSViewRepresentable {
    let content: NSAttributedString
    let search: String
    let caseSensitive: Bool
    let follow: Bool
    let searchNavigation: Int

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder

        let textView = NSTextView()
        textView.isEditable = false
        textView.isSelectable = true
        textView.isRichText = true
        textView.drawsBackground = false
        textView.textContainerInset = NSSize(width: 12, height: 12)
        textView.textContainer?.widthTracksTextView = false
        textView.textContainer?.containerSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.isHorizontallyResizable = true
        textView.isVerticallyResizable = true
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? NSTextView else { return }
        let contentChanged = !textView.attributedString().isEqual(to: content)
        if contentChanged {
            textView.textStorage?.setAttributedString(content)
            if follow { textView.scrollToEndOfDocument(nil) }
        }
        let navigationChanged = searchNavigation != context.coordinator.searchNavigation
        if search != context.coordinator.search || caseSensitive != context.coordinator.caseSensitive || navigationChanged {
            let searchChanged = search != context.coordinator.search || caseSensitive != context.coordinator.caseSensitive
            context.coordinator.search = search
            context.coordinator.caseSensitive = caseSensitive
            context.coordinator.searchNavigation = searchNavigation
            guard !search.isEmpty else { textView.setSelectedRange(NSRange(location: 0, length: 0)); return }
            let options: NSString.CompareOptions = caseSensitive ? [] : [.caseInsensitive]
            let source = textView.string as NSString
            let backwards = !searchChanged && navigationChanged && searchNavigation < context.coordinator.previousSearchNavigation
            let current = textView.selectedRange()
            let searchRange: NSRange
            if backwards {
                searchRange = NSRange(location: 0, length: min(current.location, source.length))
            } else {
                let start = searchChanged ? 0 : min(NSMaxRange(current), source.length)
                searchRange = NSRange(location: start, length: source.length - start)
            }
            var range = source.range(of: search, options: backwards ? options.union(.backwards) : options, range: searchRange)
            if range.location == NSNotFound {
                range = source.range(of: search, options: backwards ? options.union(.backwards) : options)
            }
            if range.location != NSNotFound {
                textView.setSelectedRange(range)
                textView.scrollRangeToVisible(range)
                textView.showFindIndicator(for: range)
            }
            context.coordinator.previousSearchNavigation = searchNavigation
        }
    }

    func makeCoordinator() -> Coordinator { Coordinator() }

    final class Coordinator {
        var search = ""
        var caseSensitive = false
        var searchNavigation = 0
        var previousSearchNavigation = 0
    }
}
