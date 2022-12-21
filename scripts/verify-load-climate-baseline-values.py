#! /usr/bin/env python

import logging
from argparse import ArgumentParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import pycds.climate_baseline_helpers
from pycds.climate_baseline_helpers import (
    verify_baseline_network_and_variables,
    verify_baseline_values,
)

if __name__ == "__main__":
    parser = ArgumentParser(
        description="""
Script to verify loading of climate baseline values for a single variable into a database.
Loading is presumed to have occurred from the following files or their equivalents::

storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/tmp/bc_tmax_7100.lst
storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/tmp/bc_tmin_7100.lst
storage/data/projects/PRISM/bc_climate/bc_7100_finals/station_data/ppt/bc_ppt_7100.lst

DSN strings are of form:
    dialect+driver://username:password@host:port/database
Examples:
    postgresql://scott:tiger@localhost/mydatabase
    postgresql+psycopg2://scott:tiger@localhost/mydatabase
    postgresql+pg8000://scott:tiger@localhost/mydatabase
"""
    )
    parser.add_argument("-d", "--dsn", help="Database DSN to verify")
    parser.add_argument(
        "-x",
        "--Txcount",
        help="Expected count of stations for Tx_Climatology",
        type=int,
        required=True,
    )
    parser.add_argument(
        "-n",
        "--Tncount",
        help="Expected count of stations for Tn_Climatology",
        type=int,
        required=True,
    )
    parser.add_argument(
        "-p",
        "--Precipcount",
        help="Expected count of stations for Precip_Climatology",
        type=int,
        required=True,
    )
    log_level_choices = "NOTSET DEBUG INFO WARNING ERROR CRITICAL".split()
    parser.add_argument(
        "-s",
        "--scriptloglevel",
        help="Script logging level",
        choices=log_level_choices,
        default="INFO",
    )
    parser.add_argument(
        "-e",
        "--dbengloglevel",
        help="Database engine logging level",
        choices=log_level_choices,
        default="WARNING",
    )
    args = parser.parse_args()

    script_logger = logging.getLogger(__name__)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    sa_logger = logging.getLogger("sqlalchemy.engine")
    sa_logger.addHandler(handler)
    sa_logger.setLevel(getattr(logging, args.dbengloglevel))

    script_logger = logging.getLogger(pycds.climate_baseline_helpers.__name__)
    script_logger.addHandler(handler)
    script_logger.setLevel(getattr(logging, args.scriptloglevel))

    engine = create_engine(args.dsn)
    session = sessionmaker(bind=engine)()

    verify_baseline_network_and_variables(session)

    # The following value verifications are based on manual inspection of the input files (see docstring above).
    # The expected values in `value_verification_data` are raw values copied directly from the input files, and must
    # be converted to database values according to the following rules, embodied in the `convert_x_data` functions:
    #
    # - Temperatures are coded as integers in 10ths of a degree
    # - Precipitations are coded as integers in mm
    # - For both temperatures and precipitations, the special value -9999 indicates a missing value

    def convert_temp_data(temps_in_10ths_c):
        return [v / 10.0 if v > -9999 else None for v in temps_in_10ths_c]

    def convert_precip_data(precips_in_mm):
        return [float(v) if v > -9999 else None for v in precips_in_mm]

    # from manual inspection (copy/paste) of input files:
    value_verification_data = {
        "Tx_Climatology": {
            "station_count": args.Txcount,
            "expected_stations_and_values": [
                # first 3 lines
                {
                    "station_native_id": "11",
                    "values": convert_temp_data(
                        [29, 46, 57, 89, 127, 157, 196, 201, 172, 112, 56, 30]
                    ),
                },
                {
                    "station_native_id": "16",
                    "values": convert_temp_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            173,
                            184,
                            175,
                            139,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "21",
                    "values": convert_temp_data(
                        [47, 71, 91, 127, 163, 190, 221, 223, 192, 129, 69, 41]
                    ),
                },
                # # some lines from the middle
                {
                    "station_native_id": "1101200",
                    "values": convert_temp_data(
                        [56, 76, 103, 135, 168, 194, 226, 230, 202, 141, 82, 54]
                    ),
                },
                {
                    "station_native_id": "171",
                    "values": convert_temp_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            138,
                            172,
                            200,
                            193,
                            140,
                            68,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "110CCCC",
                    "values": convert_temp_data(
                        [53, 70, 97, 134, 172, 195, 229, 231, 197, 135, 78, 49]
                    ),
                },
                {
                    "station_native_id": "101009",
                    "values": convert_temp_data(
                        [44, 60, 91, 124, 161, 191, 225, 230, 201, 135, 69, 43]
                    ),
                },
                {
                    "station_native_id": "1034175",
                    "values": convert_temp_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            169,
                            192,
                            196,
                            188,
                            145,
                            96,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "1158990",
                    "values": convert_temp_data(
                        [-36, 14, 69, 127, 181, 220, 257, 259, 198, 128, 19, -33,]
                    ),
                },
                {
                    "station_native_id": "pondosy",
                    "values": convert_temp_data(
                        [-45, -8, 32, 81, 118, 144, 177, 175, 129, 60, -11, -42]
                    ),
                },
            ],
        },
        "Tn_Climatology": {
            "station_count": args.Tncount,
            "expected_stations_and_values": [
                # first 3 lines
                {
                    "station_native_id": "11",
                    "values": convert_temp_data(
                        [-8, 1, -1, 17, 43, 65, 87, 100, 91, 47, 16, -7]
                    ),
                },
                {
                    "station_native_id": "16",
                    "values": convert_temp_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            104,
                            105,
                            89,
                            59,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "21",
                    "values": convert_temp_data(
                        [-10, -6, 0, 17, 45, 76, 96, 96, 69, 39, 10, -8]
                    ),
                },
                # # some lines from the middle
                {
                    "station_native_id": "PKM",
                    "values": convert_temp_data(
                        [-110, -100, -82, -46, -1, 32, 62, 68, 23, -24, -86, -115,]
                    ),
                },
                {
                    "station_native_id": "CLO",
                    "values": convert_temp_data(
                        [1, 11, 21, 41, 71, 99, 123, 129, 103, 65, 29, 7]
                    ),
                },
                {
                    "station_native_id": "1097646",
                    "values": convert_temp_data(
                        [-144, -113, -70, -34, 6, 36, 41, 40, 8, -24, -73, -123]
                    ),
                },
                {
                    "station_native_id": "101023",
                    "values": convert_temp_data(
                        [-6, -1, 9, 26, 57, 84, 104, 111, 90, 53, 15, -7]
                    ),
                },
                {
                    "station_native_id": "1036210",
                    "values": convert_temp_data(
                        [4, 8, 17, 39, 70, 101, 122, 123, 97, 65, 29, 9]
                    ),
                },
                {
                    "station_native_id": "1160511",
                    "values": convert_temp_data(
                        [-77, -55, -12, 28, 77, 123, 144, 134, 86, 31, -22, -70]
                    ),
                },
                {
                    "station_native_id": "1127830",
                    "values": convert_temp_data(
                        [-40, -20, 5, 35, 74, 109, 139, 137, 93, 44, 8, -29]
                    ),
                },
            ],
        },
        "Precip_Climatology": {
            "station_count": args.Precipcount,
            "expected_stations_and_values": [
                # first 3 lines
                {
                    "station_native_id": "3",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            168,
                            120,
                            82,
                            87,
                            137,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "4",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            228,
                            155,
                            104,
                            128,
                            191,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "8",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            173,
                            123,
                            85,
                            85,
                            128,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                # # some lines from the middle
                {
                    "station_native_id": "36",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            68,
                            62,
                            38,
                            49,
                            53,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "210",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            24,
                            48,
                            48,
                            36,
                            25,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "363",
                    "values": convert_precip_data(
                        [
                            -9999,
                            -9999,
                            -9999,
                            -9999,
                            61,
                            61,
                            65,
                            58,
                            75,
                            -9999,
                            -9999,
                            -9999,
                        ]
                    ),
                },
                {
                    "station_native_id": "118401",
                    "values": convert_precip_data(
                        [45, 60, 63, 85, 85, 112, 116, 103, 75, 54, -9999, 53]
                    ),
                },
                {
                    "station_native_id": "1091169",
                    "values": convert_precip_data(
                        [42, 30, 26, 21, 37, 49, 43, 44, 42, 51, 49, 46]
                    ),
                },
                {
                    "station_native_id": "1166658",
                    "values": convert_precip_data(
                        [39, 31, 33, 34, 44, 63, 54, 42, 36, 30, 36, 46]
                    ),
                },
                {
                    "station_native_id": "1108848",
                    "values": convert_precip_data(
                        [260, 232, 201, 153, 118, 96, 72, 75, 92, 204, 327, 313]
                    ),
                },
            ],
        },
    }

    # Verifications (here and in verify_baseline_values) raise AssertionError if there is a verification failure.
    # Don't let that prevent closing down the session properly afterwards.
    try:
        # Quis custodiet ipsos custodes?
        for var_name, info in value_verification_data.items():
            for esv in info["expected_stations_and_values"]:
                count = len(esv["values"])
                assert (
                    count == 12
                ), "Verification data is erroneous! Variable {}, station {}: bad count: {}".format(
                    var_name, esv["station_native_id"], count
                )

        for var_name, info in value_verification_data.items():
            script_logger.info("Verifying {}".format(var_name))
            verify_baseline_values(
                session,
                var_name,
                info["station_count"],
                info["expected_stations_and_values"],
            )

        script_logger.info("Verification complete. No problems found.")

    finally:
        session.close()
