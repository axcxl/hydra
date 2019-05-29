import exifread

class ExifInfo():
    def __init__(self, target_file):
        self.return_values = {
            "camera"        : "Image Model",
            "exp_time"      : "EXIF ExposureTime",
            "exp_fnum"      : "EXIF FNumber",
            "exp_iso"       : "EXIF ISOSpeedRatings",
            "focal_length"  : "EXIF FocalLength",
            "flash"         : "EXIF Flash",
            "lens"          : ["MakerNote LensMinMaxFocalMaxAperture", "EXIF LensSpecification"]
        }

        self.type = target_file[-3:].lower()

        self.tags = exifread.process_file(open(target_file, "rb"), details=True)

        self.output = {}
        if len(self.tags) == 0:
            self.return_empty()
        else:
            self.return_info()

    def return_empty(self):
        for elem in self.return_values:
            self.output[elem] = ""

    def return_info(self):
        for elem in self.return_values:
            exif_name = self.return_values[elem]
            # Special handling for elements with more possible values. Stops at first match
            if type(exif_name).__name__ == 'list':
                for exif_elem in exif_name:
                    try:
                        self.output[elem] = str(self.tags[exif_elem].values).strip()
                        break
                    except KeyError:
                        self.output[elem] = "ERROR"
                        pass
            else:
                try:
                    self.output[elem] = str(self.tags[exif_name].values).strip()
                except KeyError:
                    self.output[elem] = "ERROR"


    def getinfo(self):
        return self.output

