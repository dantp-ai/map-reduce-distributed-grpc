import re


def ch_lowercase_to_idx(x):
    return ord(x) - 97


def tokenize(text):
    text = text.replace("\n", " ")
    words = text.split(" ")

    return words


def filter_words(words):
    pattern = re.compile(r"^[a-z]+$")
    filtered_words = [word for word in words if pattern.match(word)]

    return filtered_words
