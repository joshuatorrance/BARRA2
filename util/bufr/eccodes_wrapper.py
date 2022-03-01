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
    def __init__(self, filepath):
        self.filepath = filepath

    def __enter__(self):
        # TODO: Update with write as an option too.
        self.file_obj = open(self.filepath, 'rb')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_obj.close()

    def get_messages(self):
        return BufrMessages(self)

    def getNumberMessages(self):
        return ecc.codes_count_in_file(self.file_obj)


class BufrMessages:
    def __init__(self, bufr):
        self.parent_bufr = bufr

        self.current_message = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_message:
            ecc.codes_release(self.current_message.message_id)

        new_message_id = ecc.codes_bufr_new_from_file(self.parent_bufr.file_obj)

        if new_message_id is None:
            self.current_message = None

            raise StopIteration
        else:
            self.current_message = BufrMessage(self.parent_bufr, new_message_id)

            return self.current_message


class BufrMessage:
    def __init__(self, bufr, message_id):
        self.parent_bufr = bufr
        self.message_id = message_id

    def get_attributes(self):
        return BufrAttributes(self)

    def get_attribute(self, key):
        return BufrAttribute(self, key)


class BufrAttributes:
    def __init__(self, bufr_message):
        self.parent_message = bufr_message

    def __iter__(self):
        self.iterator_id = ecc.codes_keys_iterator_new(self.parent_message.message_id)

        return self

    def __next__(self):
        if ecc.codes_keys_iterator_next(self.iterator_id):
            return BufrAttribute(self.parent_message, ecc.codes_keys_iterator_get_name(self.iterator_id))
        else:
            raise StopIteration

class BufrAttribute:
    def __init__(self, message, key):
        self.key = key
        self.parent_message = message

    def getValue(self):
        # TODO: There's probably a better way to do this.
        #  Can I ask the type and then use the appropriate method rather
        #  than try/except?
        if ecc.codes_is_defined(self.parent_message.message_id, self.key):
            if ecc.codes_is_missing(self.parent_message.message_id, self.key):
                return "MISSING"
            else:
                size = self.getSize()
                if size > 1:
                    return ecc.codes_get_array(self.parent_message.message_id, self.key)
                    
                else:
                    # Sometimes size is zero, codes_get seems to return None in this case
                    try:
                        return ecc.codes_get(self.parent_message.message_id, self.key)
                    except grib_errors.HashArrayNoMatchError as err:
                        # What does this error mean? No value for that attribute?
                        print("BufrAttribute.getValue:", err)

                        return None
        else:
            raise ValueError("BufrAttribute ({}) not defined.".format(self.key))


    def getSize(self):
        try:
            return ecc.codes_get_size(self.parent_message.message_id, self.key)
        except grib_errors.HashArrayNoMatchError as err:
            # What does this error mean? No value for that attribute?
            print("BufrAttribute.getSize:", err)

            return None


if __name__ == "__main__":
    test_bufr_file = "../../amsr2/test_data/AMSR2_1.bufr"

    print("Create a BufrFile object:")
    with BufrFile(test_bufr_file) as bufr_obj:
        print("\t", bufr_obj)

        print("Number of messages in file:", bufr_obj.getNumberMessages())

        limit = 10
        print("Load {} messages from the bufr".format(limit))
        i = 0
        for message in bufr_obj.get_messages():
            print(i, message)

            print("\tTest getting a particular attribute:")
            typical_year = message.get_attribute("typicalYear")
            print("\t\tTypical year:", typical_year.getValue())

            print("\tTest getting an iterator over all the attributes:")
            for attr in message.get_attributes():
                print("\t\t", attr.key)
                print("\t\t\tSize:", attr.getSize())
                print("\t\t\tValue:", attr.getValue())


            if i > limit:
                break
            else:
                i += 1

        print("Is the number of messages the same partway through the file?")
        print("Number of messages in file:", bufr_obj.getNumberMessages())

