# This is a wrapper for the ecCodes3 API to hopefully improve ease of use
# in Python and implement some more Pythonic options such as iterators
# and with statements.
#
# Intended for use at the BoM by for DA use cases.
#
# Very much a Work in Progress.
#
# The following modules are required:
# module load python3/3.8.5
# module load eccodes3
#
# Author: Joshua Torrance

# IMPORTS
from numpy import ndarray, array
import eccodes as ecc
from gribapi import errors as grib_errors
from collections.abc import Iterable
from datetime import datetime, timezone


# CLASSES
class BufrFile:
    def __init__(self, filepath, mode='rb', compressed=False):
        self.filepath = filepath
        self.filemode = mode
        self.compressed_msg = compressed

    def __enter__(self):
        self.file_obj = open(self.filepath, self.filemode)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_obj.close()

    def get_messages(self):
        return BufrMessages(self, compressed=self.compressed_msg)

    def get_number_messages(self):
        return ecc.codes_count_in_file(self.file_obj)

    def get_obs_count(self):
        count = 0
        for message in self.get_messages():
            count += message.get_obs_count()

        return count


class BufrMessages:
    def __init__(self, bufr, compressed=False):
        self.parent_bufr = bufr

        self.compressed_msg = compressed

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
            self.current_message = BufrMessage(self.parent_bufr,
                                               new_message_id,
                                               compressed=self.compressed_msg)

            return self.current_message


class BufrMessage:
    def __init__(self, bufr, message_id, compressed=False):
        self.parent_bufr = bufr
        self.message_id = message_id

        if compressed:
            ecc.codes_set(self.message_id, 'compressedData', 1)

        try:
            ecc.codes_set(self.message_id, 'unpack', 1)
        except grib_errors.FunctionNotImplementedError as e:
            #print("Failed to 'unpack', FunctionNotImplementedError:", e)
            # Trying again, sometimes it works the second time.
            ecc.codes_set(self.message_id, 'unpack', 1)

    def write_to_file(self, file_obj):
        ecc.codes_set(self.message_id, 'pack', 1)
        ecc.codes_write(self.message_id, file_obj)

    def get_attributes(self):
        return BufrAttributes(self)

    def get_attribute(self, key):
        return BufrAttribute(self, key)

    def get_value(self, key):
        return BufrAttribute(self, key).get_value()

    def get_obs_count(self):
        return BufrAttribute(self, "numberOfSubsets").get_value()

    def get_locations(self):
        try:
            lat = BufrAttribute(self, "latitude").get_value()
            lon = BufrAttribute(self, "longitude").get_value()
        except ValueError:
            lat = BufrAttribute(self, "localLatitude").get_value()
            lon = BufrAttribute(self, "localLongitude").get_value()

        num = BufrAttribute(self, "numberOfSubsets").get_value()
        if num > 1 and not isinstance(lat, Iterable):
            lat = array([lat] * num)

        if num > 1 and not isinstance(lon, Iterable):
            lon = array([lon] * num)
            
        return lat, lon

    def get_datetimes(self):
        pre = "#1#"

        def _get_time_attribute_as_iterable(key_str):
            value = BufrAttribute(self, pre+key_str).get_value()

            if isinstance(value, Iterable):
                if isinstance(value, str) and value=="MISSING":
                    return [0]
                else:
                    return value
            else:
                return [value]

        year = _get_time_attribute_as_iterable("year")
        month = _get_time_attribute_as_iterable("month")
        day = _get_time_attribute_as_iterable("day")
        hour = _get_time_attribute_as_iterable("hour")
        minute = _get_time_attribute_as_iterable("minute")
        second = _get_time_attribute_as_iterable("second")

        dt = []

        # Each datetime parameters is either a len=1 list if the value is
        # the same for all datetimes or a len=N list if the value changes.
        # E.g. year = [2015], month = [7], day = [15], hour = [18]
        #   minute = [1, 1, 1, 2, 2, 2], second = [57, 58, 59, 0, 1, 2]
        for i in range(len(second)):
            dt.append(datetime(year=year[min(i, len(year)-1)],
                               month=month[min(i, len(month)-1)],
                               day=day[min(i, len(day)-1)],
                               hour=hour[min(i, len(hour)-1)],
                               minute=minute[min(i, len(minute)-1)],
                               second=second[min(i, len(second)-1)],
                               tzinfo=timezone.utc))

        # If there's only one element multiply by the numberOfSubsets
        # because sometimes there's the same dt for all the elements
        if len(dt) == 1:
            num = BufrAttribute(self, "numberOfSubsets").get_value()
            dt = dt * num

        return dt

    def set_value(self, key, value):
        ecc.codes_set(self.message_id, key, value)


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

    def get_value(self):
        # TODO: There's probably a better way to do this.
        #  Can I ask the type and then use the appropriate method rather
        #  than try/except?
        if ecc.codes_is_defined(self.parent_message.message_id, self.key):
            if ecc.codes_is_missing(self.parent_message.message_id, self.key):
                return "MISSING"
            else:
                size = self.get_size()
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

    def get_size(self):
        try:
            return ecc.codes_get_size(self.parent_message.message_id, self.key)
        except grib_errors.HashArrayNoMatchError as err:
            # What does this error mean? No value for that attribute?
            print("BufrAttribute.getSize:", err)

            return None


if __name__ == "__main__":
    # I want to truncate the printing of numpy arrays.
    import numpy
    numpy.set_printoptions(threshold=5)

    test_bufr_file = "../../amsr2/test_data/AMSR2_1.bufr"

    print("Create a BufrFile object:")
    with BufrFile(test_bufr_file) as bufr_obj:
        print("\t", bufr_obj)

        print("Number of messages in file:", bufr_obj.get_number_messages())

        limit = 10
        print("Load {} messages from the bufr".format(limit))
        i = 0
        for msg in bufr_obj.get_messages():
            print(i, msg)

            print("\tTest getting a particular attribute:")
            typical_year = msg.get_attribute("typicalYear")
            print("\t\tTypical year:", typical_year.get_value())

            print("\tTest getting an iterator over all the attributes:")
            for attr in msg.get_attributes():
                print("\t\t", attr.key)
                
                if attr.key == "sequences":
                    print("\t\t\tSkipping sequences for now. It's troublesome.")
                    continue
                
                sze = attr.get_size()
                print("\t\t\tSize:", sze)
                
                value = attr.get_value()
                print("\t\t\tValue:", numpy.array(value))

                print("\t\t\tType:", type(value))

            if i > limit:
                break
            else:
                i += 1

        print("Is the number of messages the same partway through the file?")
        print("Number of messages in file:", bufr_obj.get_number_messages())

