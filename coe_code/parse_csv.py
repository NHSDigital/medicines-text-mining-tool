"""Parse homecare csv to produce list of unique names"""
import argparse
import csv

def parse_hah(args: argparse.Namespace):
    lines = []
    with open(args.input_file, 'r', encoding='utf-8') as input_file:
        reader = csv.reader(input_file)
        heads = next(reader)
        for line in reader:
            lines.append(line)
    name2dmd = {}
    for line in lines:
        name = line[1]
        dmd = line[2]
        if not name in name2dmd:
            name2dmd[name] = dmd
    with open(args.output_file, 'w', encoding='utf-8', newline='') as output_file:
        writer = csv.writer(output_file)
        if args.export_dmd:
            writer.writerow(["medication_name_value", "form_in_test", "dm+d"])
        else:
            writer.writerow(["medication_name_value", "form_in_text"])
        for name, dmd in name2dmd.items():
            if args.reject_bad_dmd and dmd.startswith('0'):
                dmd = ''
            if args.export_dmd:
                writer.writerow([name, dmd])
            else:
                writer.writerow([name])

def parse_lloyds(args: argparse.Namespace):
    lines = []
    with open(args.input_file, 'r', encoding='utf-8') as input_file:
        reader = csv.reader(input_file)
        heads = next(reader)
        for line in reader:
            lines.append(line)
    names = set([l[args.input_name_column] for l in lines])
    with open(args.output_file, 'w', encoding='utf-8', newline='') as output_file:
        writer = csv.writer(output_file)
        if args.export_dmd:
            writer.writerow(["medication_name_value", "form_in_text", "dm+d"])
        else:
            writer.writerow(["medication_name_value", "form_in_text"])
        dmd = ''
        for name in names:
            if "REGIME" in name:
                continue
            if args.export_dmd:
                writer.writerow([name, dmd])
            else:
                writer.writerow([name])

F_DICT = {
    'lloyds': parse_lloyds,
    'hah': parse_hah
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input-file",
        type=str,
        required=True,
        action="store",
        dest="input_file",
        help="Input .csv file name"
    )
    parser.add_argument(
        "-o", "--output-file",
        type=str,
        required=True,
        action="store",
        dest="output_file",
        help="Output .csv file name"
    )
    parser.add_argument(
        "-t", "--input-file-type",
        type=str,
        required=True,
        action="store",
        dest="input_file_type",
        choices=["lloyds", "hah"],
        help="Input csv file type (lloyds or hah)"
    )
    parser.add_argument(
        "-r", "--reject-bad-dmd",
        action="store_true",
        dest="reject_bad_dmd",
        help="Whether or not to reject bad dmd when dmd present. Bad dmd=dmd starting with '0'"
    )
    parser.add_argument(
        "-d", "--export-dmd",
        action="store_true",
        dest="export_dmd",
        help="Whether or not to include a dm+d column in the output"
    )
    parser.add_argument(
        "-c", "--name-column",
        type=int,
        required=True,
        action="store",
        dest="input_name_column",
        help="Column index of the name"
    )
    args = parser.parse_args()
    print(args)
    F_DICT[args.input_file_type](args)


if __name__ == '__main__':
    main()