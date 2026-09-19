//! Composer cursor positions are character indices, never UTF-8 byte offsets.
pub(super) fn chars(s: &str) -> usize {
    s.chars().count()
}

pub(super) fn char_byte_index(s: &str, index: usize) -> usize {
    s.char_indices()
        .nth(index)
        .map_or(s.len(), |(byte, _)| byte)
}

pub(super) fn insert_at(s: &mut String, cursor: &mut usize, c: char) {
    s.insert(char_byte_index(s, *cursor), c);
    *cursor += 1;
}

pub(super) fn paste_text(s: &mut String, cursor: &mut usize, text: &str) {
    s.insert_str(char_byte_index(s, *cursor), text);
    *cursor += chars(text);
}

pub(super) fn backspace_at(s: &mut String, cursor: &mut usize) {
    if *cursor > 0 {
        *cursor -= 1;
        delete_at(s, cursor);
    }
}

pub(super) fn delete_at(s: &mut String, cursor: &mut usize) {
    let byte = char_byte_index(s, *cursor);
    if byte < s.len() {
        s.remove(byte);
    }
}

pub(super) fn delete_word_left(s: &mut String, cursor: &mut usize) {
    let end = char_byte_index(s, *cursor);
    move_word_left(s, cursor);
    s.drain(char_byte_index(s, *cursor)..end);
}

pub(super) fn move_word_left(s: &str, cursor: &mut usize) {
    let prefix = &s[..char_byte_index(s, *cursor)];
    let mut chars = prefix.chars().rev().peekable();
    while chars.peek().is_some_and(|c| c.is_whitespace()) {
        chars.next();
        *cursor -= 1;
    }
    while chars.peek().is_some_and(|c| !c.is_whitespace()) {
        chars.next();
        *cursor -= 1;
    }
}

pub(super) fn move_word_right(s: &str, cursor: &mut usize) {
    let suffix = &s[char_byte_index(s, *cursor)..];
    let mut chars = suffix.chars().peekable();
    while chars.peek().is_some_and(|c| c.is_whitespace()) {
        chars.next();
        *cursor += 1;
    }
    while chars.peek().is_some_and(|c| !c.is_whitespace()) {
        chars.next();
        *cursor += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        backspace_at, char_byte_index, delete_at, delete_word_left, insert_at, move_word_right,
        paste_text,
    };

    #[test]
    fn utf8_edit_sequence_uses_char_not_byte_positions() {
        let mut text = "\u{1f680}\u{e9}x".to_owned();
        assert_eq!(
            (0..5)
                .map(|i| char_byte_index(&text, i))
                .collect::<Vec<_>>(),
            vec![0, 4, 6, 7, 7]
        );
        let mut cursor = 2;
        insert_at(&mut text, &mut cursor, '!');
        assert_eq!(text, "\u{1f680}\u{e9}!x");
        backspace_at(&mut text, &mut cursor);
        delete_at(&mut text, &mut cursor);
        paste_text(&mut text, &mut cursor, " two\nthree");
        delete_word_left(&mut text, &mut cursor);
        assert_eq!(text, "\u{1f680}\u{e9} two\n");
        cursor = 0;
        move_word_right(&text, &mut cursor);
        assert_eq!(cursor, 2);
    }
}
