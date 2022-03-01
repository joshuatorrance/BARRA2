# This is a wrapper for the ecCodes3 API to hopefully improve ease of use
# in Python and implement some more Pythonic options such as iterators
# and with statements.
#
# Intended for use at the BoM by for DA use cases.
#
# Very much a Work in Progress.
#
# Author: Joshua Torrance

# IMPORTS
import eccodes as ecc
from gribapi import errors as grib_errors


# CLASSES
class BufrFile:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        # TODO: Update with write as an option too.
        self.file_obj = open(self.filename, 'rb')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_obj.close()

    def get_messages(self):
        """
        Return an  interator that provides the messages for the extant bufr file.

        :return: An iterator that returns the ecCodes message ids.
        """
        return BufrMessages(self)


class BufrMessages:
    def __init__(self, bufr):
        self.parent_bufr = bufr

        self.cur_message_id = None
        self.cur_message = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.cur_message_id:
            ecc.codes_release(self.cur_message_id)

        new_message_id = ecc.codes_bufr_new_from_file(self.parent_bufr.file_obj)

        if new_message_id is None:
            self.cur_message = None

            raise StopIteration
        else:
            self.cur_message = BufrMessage(self.parent_bufr, new_message_id)

            return self.cur_message


class BufrMessage:
    def __init__(self, bufr, message_id):
        self.parent_bufr = bufr
        self.message_id = message_id

    def get_attributes(self):
        return BufrAttributes(self)


class BufrAttributes:
    def __init__(self, bufr_message):
        self.parent_bufr_message = bufr_message

    def __iter__(self):
        self.iterator_id = ecc.codes_bufr_keys_iterator_new(self.parent_bufr_message.message_id)

        return self

    def __next__(self):
        if ecc.codes_bufr_keys_iterator_next(self.iterator_id):
            return BufrAttribute(self.parent_bufr_message, ecc.codes_keys_iterator_get_name(self.iterator_id))
        else:
            raise StopIteration

class BufrAttribute:
    def __init__(self, bufr, key):
        self.key = key
        self.parent_bufr = bufr

    def getValue(self):
        # TODO: There's probably a better way to do this.
        #  Can I ask the type and then use the appropriate method rather
        #  than try/except?
        try:
            return ecc.codes_get(self.parent_bufr, self.key)
        except grib_errors.ArrayTooSmallError:
            return ecc.codes_get_array(self.parent_bufr, self.key)


if __name__ == "__main__":
    test_bufr_file = "../../amsr2/test_data/AMSR2_1.bufr"

    with BufrFile(test_bufr_file) as bufr_obj:
        print(bufr_obj)

        i = 0
        for message in bufr_obj.get_messages():
            print(message)

            for attr in message.get_attributes():
                print(attr.key())


            if i > 10:
                break
            else:
                i += 1

