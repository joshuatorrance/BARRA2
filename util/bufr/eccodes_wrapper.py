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

    def __iter__(self):
        return self

    def __next__(self):
        if self.cur_message_id:
            ecc.codes_release(self.cur_message_id)

        self.cur_message_id = ecc.codes_bufr_new_from_file(self.parent_bufr.file_obj)

        if self.cur_message_id is None:
            raise StopIteration
        else:
            return self.cur_message_id


# TODO: get BufrMessages to return a BufrMessage so that said message can be
#  asked for it's BufrAttributes (or header or...).
class BufrAttributes:
    def __init__(self, bufr_message):
        self.parent_bufr_message = bufr_message

    def __iter__(self):
        iterator_id = ecc.codes_keys_iterator_new(self.parent_bufr_message.cur_message_id)

        self.iterator_id = iterator_id

    def __next__(self):
        if ecc.codes_bufr_keys_iterator_next(self.iterator_id):
            return ecc.codes_keys_iterator_get_name(self.iterator_id)
        else:
            raise StopIteration


if __name__ == "__main__":
    test_bufr_file = "../../amsr2/test_data/AMSR2_1.bufr"

    with BufrFile(test_bufr_file) as bufr_obj:
        print(bufr_obj)

        i = 0
        for message in bufr_obj.get_messages():
            print(message)

            if i > 10:
                break
            else:
                i += 1

