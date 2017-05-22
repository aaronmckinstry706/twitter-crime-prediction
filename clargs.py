import operator

def process_argv(argv):
    if len(argv) == 4:
        if reduce(operator.and_, [string_is_int(s) for s in argv[1:]]):
            year = int(argv[1])
            month = int(argv[2])
            day = int(argv[3])
            if 2015 <= year and year <= 2016:
                if 1 <= month and month <= 12:
                    if 1 <= day and day <= 31:
                        return year, month, day
    
    return None

def string_is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

