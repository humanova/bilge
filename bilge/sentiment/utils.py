# (c) 2021 Emir Erbasan (humanova)
import logging

ignore_list = ['http', 'www.', '---']
modify_list = [('@', '@user')]


def preprocess(text):
    new_text = []
    for t in text.split(" "):
        curr_t = None

        for c, m in modify_list:
            if t.startswith(c):
                curr_t = m
                break

        for ig_text in ignore_list:
            if t.startswith(ig_text):
                curr_t = ''
                break

        new_text.append(t) if curr_t is None else new_text.append(curr_t)
    return " ".join(new_text)
