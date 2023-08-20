

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def crop_length(st, ed, c_st=None, c_ed=None):
    if not c_st:
        c_st_ = st
    else:
        c_st_ = c_st

    if not c_ed:
        c_ed_ = ed
    else:
        c_ed_ = c_ed

    if c_st_ > c_ed_:
        return 0
    if c_st_ > ed:
        return 0
    if c_ed_ < st:
        return 0

    return min(ed, c_ed_) - max(st, c_st_)


def crop(st, ed, c_st, c_ed):
    if not c_st:
        c_st_ = st
    else:
        c_st_ = c_st

    if not c_ed:
        c_ed_ = ed
    else:
        c_ed_ = c_ed

    if c_st_ < st and c_ed_ < st:
        return st, st

    if c_st_ > ed and c_ed_ > ed:
        return ed, ed

    return min(max(st, c_st_), ed), max(min(ed, c_ed_), st)


if __name__ == '__main__':
    print(crop(1, 2, 3, 4))
    print(crop(1, 2, -1, -2))
    print(crop(1, 4, -1, 2))
    print(crop(1, 4, 2, -1))
    print(crop(1, 4, 3, 5))
    print(crop(1, 4, 5, 3))











