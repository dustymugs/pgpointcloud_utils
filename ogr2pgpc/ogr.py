import datetime

class OGR_TZ(datetime.tzinfo):

    def __init__(self, ogr_tz):

        self._hours = None
        self._minutes = None

        if ogr_tz <= 1:
            return

        offset = int(ogr_tz - 100 * 15)
        hours = int(offset / 60)
        minutes = int(abs(offset - hours * 60))

        if offset < 0:
            self._hours = -1 * abs(hours)
            self._minutes = -1 * minutes
        else:
            self._hours = hours
            self._minutes = minutes

    def utcoffset(self, dt):

        if self._hours is None:
            return None

        return datetime.timedelta(hours=self._hours, minutes=self._minutes)

    def tzname(self, dt):

        if self._hours is None:
            return None

        sign = '-' if self._hours < 0 else '+'

        return "%s%02f:%02f" % (
            sign,
            abs(self._hours),
            abs(self._minutes)
        )

    def dst(self, dt):
        return timedelta(0)

