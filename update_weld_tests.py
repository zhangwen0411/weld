#!/usr/bin/env python3
"""
Updates the generated Spatial code in the example files.
"""

from glob import glob
import subprocess
import sys


def gen_spatial(weld_code, indent, new_contents):
    repl_input = weld_code.encode("utf-8")
    try:
        result = subprocess.run(["target/debug/repl", "--spatial"],
                    input=repl_input, stdout=subprocess.PIPE, check=True)
    except subprocess.CalledProcessError:
        sys.exit("\tWeld repl failed!")

    repl_output = result.stdout.decode("utf-8")
    spatial_started = False
    for line in repl_output.splitlines():
        if spatial_started:
            new_contents.append((indent + line).rstrip())
            if line.rstrip() == "}":
                break
        elif line.startswith("Spatial code:"):
            spatial_started = True

    if not spatial_started:
        sys.exit("\tNo Spatial code generated!")


def main():
    if '--no-build' not in sys.argv:
        make_result = subprocess.call(["cargo", "build"])
        if make_result != 0:
            sys.exit("cargo build failed!")

    for filename in glob("examples/spatial/*.scala"):
        print("Updating %s..." % filename, file=sys.stderr)
        new_contents = []
        weld_code = None
        with open(filename, "r") as f:
            ignore_lines = False
            for line in f:
                if not ignore_lines:
                    new_contents.append(line)
                    if line.strip().startswith("//w"):
                        if weld_code is not None:
                            sys.exit("\tFound more than one Weld snippet!")
                        weld_code = line.split("//w")[1].strip()
                        indent = line[:line.find("/")]
                        gen_spatial(weld_code, indent, new_contents)

                        ignore_lines = True
                elif line.rstrip() == indent + "}":
                    ignore_lines = False

        if weld_code is None:
            sys.exit("\tWeld snippet not found!")

        with open(filename, "w") as f:
            for line in new_contents:
                print(line.rstrip(), file=f)

        print("\tDone!", file=sys.stderr)


if __name__ == '__main__':
    main()
