import SwiftUI

struct ANSIParser: Sendable {
    private var style = ANSIStyle()
    private var pending: [UInt8] = []

    mutating func consume(_ input: String) -> AttributedString {
        let bytes = pending + Array(input.utf8)
        pending.removeAll(keepingCapacity: true)
        var result = AttributedString()
        var plain: [UInt8] = []
        var index = 0

        func appendPlain(_ bytes: inout [UInt8], style: ANSIStyle, to result: inout AttributedString) {
            guard !bytes.isEmpty else { return }
            var attributes = AttributeContainer()
            attributes.foregroundColor = style.foreground
            attributes.backgroundColor = style.background
            attributes.font = style.font
            attributes.underlineStyle = style.underline ? .single : nil
            result.append(AttributedString(String(decoding: bytes, as: UTF8.self), attributes: attributes))
            bytes.removeAll(keepingCapacity: true)
        }

        while index < bytes.count {
            let byte = bytes[index]
            if byte == 0x1b {
                appendPlain(&plain, style: style, to: &result)
                guard index + 1 < bytes.count else { pending = [byte]; break }
                let introducer = bytes[index + 1]
                if introducer == 0x5b {
                    var end = index + 2
                    while end < bytes.count, end - index <= 64, !(0x40...0x7e).contains(bytes[end]) { end += 1 }
                    guard end < bytes.count else {
                        if bytes.count - index <= 64 { pending = Array(bytes[index...]) }
                        break
                    }
                    guard end - index <= 64 else {
                        index = end + 1
                        continue
                    }
                    if bytes[end] == 0x6d {
                        let parameters = String(decoding: bytes[(index + 2)..<end], as: UTF8.self)
                        applySGR(parameters)
                    }
                    index = end + 1
                    continue
                }
                if [0x5d, 0x50, 0x5e, 0x5f].contains(introducer) {
                    var end = index + 2
                    var terminated = false
                    while end < bytes.count, end - index <= 256 {
                        if bytes[end] == 0x07 { end += 1; terminated = true; break }
                        if bytes[end] == 0x1b, end + 1 < bytes.count, bytes[end + 1] == 0x5c { end += 2; terminated = true; break }
                        end += 1
                    }
                    if !terminated {
                        if bytes.count - index <= 256 { pending = Array(bytes[index...]) }
                        break
                    }
                    index = end
                    continue
                }
                index += 2
                continue
            }
            if byte == 0x09 || byte == 0x0a || byte == 0x0d || byte >= 0x20 { plain.append(byte) }
            else { plain.append(contentsOf: Array("�".utf8)) }
            index += 1
        }
        appendPlain(&plain, style: style, to: &result)
        return result
    }

    static func plain(_ input: String) -> String {
        var parser = ANSIParser()
        return String(parser.consume(input).characters)
    }

    private mutating func applySGR(_ parameters: String) {
        let values = parameters.isEmpty ? [0] : parameters.split(separator: ";", omittingEmptySubsequences: false).compactMap { Int($0) }
        for value in values {
            switch value {
            case 0: style = ANSIStyle()
            case 1: style.bold = true
            case 2: style.dim = true
            case 4: style.underline = true
            case 22: style.bold = false; style.dim = false
            case 24: style.underline = false
            case 30...37, 90...97: style.foregroundCode = value
            case 39: style.foregroundCode = nil
            case 40...47, 100...107: style.backgroundCode = value
            case 49: style.backgroundCode = nil
            default: continue
            }
        }
    }
}

private struct ANSIStyle: Sendable {
    var foregroundCode: Int?
    var backgroundCode: Int?
    var bold = false
    var dim = false
    var underline = false
    var foreground: Color { color(foregroundCode, background: false) ?? .primary.opacity(dim ? 0.65 : 1) }
    var background: Color? { color(backgroundCode, background: true) }
    var font: Font { bold ? .system(.body, design: .monospaced).bold() : .system(.body, design: .monospaced) }

    private func color(_ code: Int?, background: Bool) -> Color? {
        guard var value = code else { return nil }
        if background { value -= value >= 100 ? 10 : 10 }
        let bright = value >= 90
        let index = bright ? value - 90 : value - 30
        let colors: [Color] = [.primary, .red, .green, .yellow, .blue, .purple, .cyan, .secondary]
        guard colors.indices.contains(index) else { return nil }
        return colors[index].opacity(bright ? 1 : 0.85)
    }
}
