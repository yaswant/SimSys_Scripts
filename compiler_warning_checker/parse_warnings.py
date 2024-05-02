#!/usr/bin/env python3

import os

class Warning:
    """Class to describe detection and extraction of a message
    warning from the GNU compiler"""
  
    def __init__(self):
        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'Bar': (1,1)}

    def foundmessage(self,line):
        """Takes line(str) and returns True if a message is found"""
        if line.find(self.startstr) != -1:
            return True
        else:
            return False

    def getoffsets(self,reflineno):
        """Takes reflineno(int) and line(str)
        Returns two integers
        """

        warningtype = "default"

        #Overide the warning type as needed by searching line

        startline = reflineno + self.offsetdict[warningtype][0]
        if startline <= 0:
            raise ValueError("Start line less than zero")
        
        endline = reflineno + self.offsetdict[warningtype][1]
        if endline <= 0:
            raise ValueError("End line less than zero")
        
        return startline, endline
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        """
        return lines


class GnuWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the GNU compiler"""
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = "Warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (-4,1)}
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #-path to the source file in the first line
        #-self.startstr in the last line

        if lines[0].find("/src/") == -1:
            print(lines)
            raise ValueError("Expecting first line of warning to contain /src/")

        if lines[-1].find(self.startstr) == -1:
            print(lines)
            raise ValueError("Expecting line of warning to contain " + self.startstr)

        return lines

#Needs tuning
class CCEWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the CCE compiler"""
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (1,1)}
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #None yet implemented

        return lines

#Needs tuning
class PGIWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the PGI compiler"""
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (1,1)}
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #None yet implemented

        return lines

#Needs tuning
class IntelWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the PGI compiler"""
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (1,1)}
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #None yet implemented

        return lines

#Needs tuning
class NAGWarning(Warning):
    """Class to describe detection and extraction of a message
    warning from the PGI compiler"""
  
    def __init__(self):
        super().__init__()

        #String to detect a warning
        self.startstr = "warning:"

        #Dictionary of line offsets as these may vary depending on the warning type
        self.offsetdict = {'default': (1,1)}
    
    def getmessage(self,lines):
        """Takes lines(list(str))
        Returns list(str)
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #None yet implemented

        return lines

#Stolen from suite_report.py
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
            startline, endline = searchobj.getoffsets(lineno)
            messagesout.append(searchobj.getmessage(linesin[startline:endline]))
        #end if

        lineno += 1
    #end for

    return messagesout


def main():
    """Main program.
    Parses fcm-make.log files and filters out compiler warnings"""

    cylc_run = "/net/data/users/hadgr/cylc-run"
    run_name = "vn13.5_scm_warnings/run4"

    #Search through for appropriate tasks
    for dir in os.listdir(cylc_run + "/" + run_name + "/" + "log/job/1/"):
        if dir.find("fcm_make_") != -1 and not dir.find("install_ctldata") != -1 and not dir.find("fcm_make_drivers") != -1:
            task_name = dir

            print("======= Processing " + task_name + " =======" )
            
            filename = cylc_run + "/" + run_name + "/" + "log/job/1/" + task_name + "/01/fcm-make.log"

            #Work out which compiler we're working with
            #Switching for different versions could be done here or in the classes
            if task_name.find("_gnu_") != -1:
                searchparams = GnuWarning()
            elif task_name.find("_cce_") != -1:
                searchparams = CCEWarning()
            elif task_name.find("_pgi_") != -1:
                searchparams = PGIWarning()
            elif task_name.find("_intel_") != -1 or task_name.find("_ifort_") != -1:
                searchparams = IntelWarning()
            elif task_name.find("_nag_") != -1:
                searchparams = NAGWarning()
            else:
                print(filename)
                raise ValueError("Unable to determine compiler")

            raw_lines = _read_file(filename)

            extracted_lines = _extract_line_start(raw_lines,"[>>&2]")

            extracted_messages = _find_message(extracted_lines,searchparams)

            print(task_name + ": Extacted " + str(len(extracted_messages)) + " compiler warnings")
        #end if
    #end for

    return

if __name__ == "__main__":
    main()
