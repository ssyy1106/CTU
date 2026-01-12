class Shift:
    def __init__(self, date = "", shiftBegin = "", shiftEnd = "", lunchMinute = 30):
        self.date = date
        self.begin = shiftBegin
        self.end = shiftEnd
        self.lunchMinute = lunchMinute

class Punch:
    def __init__(self, date = "", time = ""):
        self.date = date
        self.time = time

class PunchProblem:
    def __init__(self, date = "", totalHours = ""):
        self.date = date
        self.totalHours = totalHours