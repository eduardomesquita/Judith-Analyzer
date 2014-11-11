# -*- coding: utf-8 -*-
from unicodedata import normalize


def clear_coding( txt, codif='utf-8'):
    try:
        return normalize('NFKD', txt.decode(codif)).encode('ASCII','ignore')
    except Exception as ex:
        return txt