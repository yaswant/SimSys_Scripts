#!/usr/bin/env python3

import os

class GnuWarning:
    """Class to describe detection and extraction of a message
    warning from the GNU compiler"""
  
    #String to detect a warning
    startstr = "Warning:"

    #Dictionary of line offsets as these may vary depending on the warning type
    offsetdict = {'default': (-4,1)}

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
        
        Could be embellished to curate the message further
        """

        #Basic checks- for this compiler we expect:
        #-path to the source file in the first line
        #-self.startstr in the last line

        if not lines[0].find("/src/"):
            print(lines[0])
            raise ValueError("Expecting first line of warning to contain /src/")

        if not lines[-1].find(self.startstr):
            raise ValueError("Expecting line of warning to contain " + self.startstr)

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
 
    filename = "/net/data/users/hadgr/cylc-run/vn13.5_scm_warnings/run4/log/job/1/fcm_make_xc40_gnu_um_rigorous_omp/01/fcm-make.log"

    #Instantiate an object that can search and return the warnings.
    #Presently a simple class but could get into inheritance and stuff
    searchparams = GnuWarning()

    raw_lines = _read_file(filename)

    extracted_lines = _extract_line_start(raw_lines,"[>>&2]")

    extracted_messages = _find_message(extracted_lines,searchparams)

    print("Extacted " + str(len(extracted_messages)) + " compiler warnings")

    return

if __name__ == "__main__":
    main()
