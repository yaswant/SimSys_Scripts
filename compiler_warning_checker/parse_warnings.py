#!/usr/bin/env python3

import os

class Warning:
    """Parent class to describe detection and extraction of a message
    warning from a generic compiler"""
  
    def __init__(self):
        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (0,3)}

    def foundmessage(self,line):
        """Takes line(str) and returns True if a message is found"""
        if line.find(self.startstr) != -1:
            #Add special cases to ignore specific warnings common to many compilers
            if line.find("warning: missing terminating") != -1 or \
               line.find("statically linked applications requires") != -1:
                return False
            else:
                return True
        else:
            return False

    def getoffsets(self,reflineno,line):
        """Takes reflineno(int) and line(str)
        Returns two integers
        """
        warningtype = "default"

        startline = reflineno + self.offsetdict[warningtype][0]
        if startline < 0:
            raise ValueError("Start line less than zero")
        
        endline = reflineno + self.offsetdict[warningtype][1]
        if endline < 0:
            raise ValueError("End line less than zero")
        
        return startline, endline
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        """
        return lines

class AOCCWarning(Warning):
    """Child of Warning for the AOCC compiler"""
    def __init__(self):
        super().__init__()

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (0,3), 'transform': (0,2)}

    def getoffsets(self,reflineno,line):
        """Takes reflineno(int) and line(str)
        Returns two integers
        """
        warningtype = "default"

        if line.find("-Wpass-failed=transform-warning"):
            warningtype = "transform"

        startline = reflineno + self.offsetdict[warningtype][0]
        if startline < 0:
            raise ValueError("Start line less than zero")
        
        endline = reflineno + self.offsetdict[warningtype][1]
        if endline < 0:
            raise ValueError("End line less than zero")
        
        return startline, endline


#Needs tuning- rank and type mismatched broken
class GnuWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the GNU compiler
    
    GNU is different from other compilers with the warning format

    Start of each warning has .F90:239:21: Look for a .F90 and tailing colon
    End of each warning has Warning:
    """
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = ".F90:"
        self.endstr = "Warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (0,20)}

    def foundmessage(self,line):
        """Takes line(str) and returns True if a message is found"""
        if line.find(self.startstr) != -1 and line[-2:-1] == ":":
            return True
        else:
            return False

    def getoffsets(self,reflineno,line):
        """Takes reflineno(int) and line(str)
        Returns two integers
        """
        warningtype = "default"

        startline = reflineno + self.offsetdict[warningtype][0]
        if startline <= 0:
            raise ValueError("Start line less than zero")
        
        endline = reflineno + self.offsetdict[warningtype][1]
        if endline <= 0:
            raise ValueError("End line less than zero")
        
        return startline, endline

    def getmessage(self,lines,sourcestr="/src"):
        """Takes lines(list(str)) and optionally sourcestr
        Returns list(str)

        Performs some basic sanity checking on the output        
        #-path fragment to the source file in the first line
        #-self.startstr in the last line
        """
        
        if lines[0].find(sourcestr) == -1:
            raise ValueError("Expecting first line of warning to contain /src/")

        #Default case for warnings
        iline = 0
        for line in lines:
            iline += 1
            if line.find(self.endstr) != -1:
                return lines[0:iline]
        
        #Special case for "missing terminating character" warnings
        if lines[1].find("warning: missing terminating") != -1:
            return lines[0:4]
 
        #Last resort is to package up the lot and say we failed to parse
        returnlist = []
        for line in lines:
            returnlist.append(line)
        returnlist.append("Unable to parse\n")
        
        return returnlist

def _read_file(filename):
    """Takes filename (str)
    Return contents of a file, as list of strings."""
    if os.path.exists(filename):
        with open(filename, "r") as filehandle:
            lines = filehandle.readlines()
    else:
        print('[ERROR] Unable to find file :\n    "{0:s}"'.format(filename))
        raise IOError(
            '_read_file got invalid filename : "{0:s}"'.format(filename)
        )
    return lines


def _extract_line_start(linesin,searchstr):
    """Takes linesin (list[str]) and searchstr(str).
    Returns a list of all items in linesin starting with searchstr"""

    linesout = []
    for line in linesin:
        if  line[0:len(searchstr)] == searchstr:
            linesout.append(line)
    
    return linesout


def _find_message(linesin,searchobj):
    """Takes linesin (list[str]) and searchobj
    Returns a list of lists containing the messages according to the definition
    in seachobj"""

    messagesout = []

    lineno = 0
    for line in linesin:
        if searchobj.foundmessage(line):
            startline, endline = searchobj.getoffsets(lineno,line)
            messagesout.append(searchobj.getmessage(linesin[startline:endline]))
        #end if

        lineno += 1
    #end for

    return messagesout

def _write_warnings(messages, filename, path="./", newline=False):
    """Takes messages(list(list(str)) and writes to a file
    The optional newline argument adds a newline at the end of each
    element of the list.
    """
    retn = "\n" if newline else ""

    with open(path + "/" + filename, "w") as filehandle:
        for lines in messages:
            filehandle.write("{0:s}{1:s}".format("========\n", retn))
            for line in lines:
                filehandle.write("{0:s}{1:s}".format(line, retn))


def _write_file(filename, lines, newline=False):
    """Takes filemname and list of strings and opt newline boolean.
    Writes array to file of given name.
    The optional newline argument adds a newline at the end of each
    element of the list.
    Returns None"""
    retn = "\n" if newline else ""
    with open(filename, "w") as filehandle:
        for line in lines:
            filehandle.write("{0:s}{1:s}".format(line, retn))


def main():
    """Main program.
    Parses fcm-make.log files and filters out compiler warnings"""

    # cylc_run = "/net/data/users/hadgr/cylc-run"
    # run_name = "vn13.5_scm_warnings/run4"

    cylc_run = "/home/h01/frzz/cylc-run"
    run_name = "um_heads_nightly_2024-05-16/run1"

    #Search through for appropriate tasks
    for dir in os.listdir(cylc_run + "/" + run_name + "/" + "log/job/1/"):
        if dir.find("fcm_make_") != -1 and not \
           dir.find("install_ctldata") != -1 and not \
           dir.find("_drivers") != -1 and not \
           dir.find("_mirror_") != -1 and not \
           dir.find("_extract_") != -1:
            task_name = dir

            print("======= Processing " + task_name + " =======" )
            
            filename = cylc_run + "/" + run_name + "/" + "log/job/1/" + task_name + "/NN/fcm-make.log"

            print(filename)

            #Work out which compiler we're working with
            #Switching for different versions could be done here or in the classes
            if task_name.find("_gnu_") != -1:
                searchparams = GnuWarning()
            elif task_name.find("_cce_") != -1:
                searchparams = Warning()
            elif task_name.find("_pgi_") != -1:
                searchparams = Warning()
            elif task_name.find("_intel_") != -1 or task_name.find("_ifort_") != -1:
                searchparams = Warning()
            elif task_name.find("_nag_") != -1:
                searchparams = Warning()
            elif task_name.find("_aocc_") != -1:
                searchparams = AOCCWarning()
            elif task_name.find("_nvidia_") != -1:
                searchparams = Warning()
            else:
                print(filename)
                raise ValueError("Unable to determine compiler")

            raw_lines = _read_file(filename)

            extracted_lines = _extract_line_start(raw_lines,"[>>&2]")

            extracted_messages = _find_message(extracted_lines,searchparams)

            outputfile = task_name + ".txt"

            _write_warnings(extracted_messages,outputfile)

        #end if
    #end for

    return

if __name__ == "__main__":
    main()
