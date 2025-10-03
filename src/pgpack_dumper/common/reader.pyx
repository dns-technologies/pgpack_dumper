cdef class CopyReader:
    """Read from iterable Copy object."""

    def __init__(
        self,
        object copyobj,
    ):
        """Class initialization."""

        self.copyobj = copyobj
        self.iterator = iter(self.copyobj.__enter__())
        self.bufferobj = bytearray()
        self.closed = False

    cpdef bytes read(self, long long size):
        """Read from copy."""

        if size <= 0:
            return b""

        cdef object chunk
        cdef Py_ssize_t buffer_len

        try:
            while len(self.bufferobj) < size:
                chunk = next(self.iterator)
                self.bufferobj.extend(chunk)

            result = bytes(self.bufferobj[:size])
            del self.bufferobj[:size]
            return result

        except StopIteration:
            self.close()
            buffer_len = len(self.bufferobj)

            if buffer_len > 0:
                if size >= buffer_len:
                    result = bytes(self.bufferobj)
                    self.bufferobj = bytearray()
                else:
                    result = bytes(self.bufferobj[:size])
                    del self.bufferobj[:size]
                return result
            return b""

    cpdef void close(self):
        """Close CopyReader."""

        if not self.closed:
            self.copyobj.__exit__(None, None, None)
            self.closed = True
