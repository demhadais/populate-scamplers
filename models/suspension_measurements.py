def _parse_concentration(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
    biological_material: str | None = None,
    instrument_name: str | None = None,
    counting_method: str | None = None,
) -> dict[str, Any] | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    if counting_method is not None:
        parsed_counting_method = CellCountingMethod(to_snake_case(counting_method))
    else:
        parsed_counting_method = None

    if biological_material is None:
        biological_material = BiologicalMaterial(
            to_snake_case(row["biological_material"])
        )

    unit = (biological_material, VolumeUnit.Millliter)

    return SuspensionMeasurementFields.Concentration(
        measured_at=measured_at,
        instrument_name=instrument_name,
        counting_method=parsed_counting_method,
        unit=unit,
        value=value,
    )


def _parse_volume(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
) -> SuspensionMeasurementFields.Volume | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    return SuspensionMeasurementFields.Volume(
        measured_at=measured_at, unit=VolumeUnit.Microliter, value=value
    )


def _parse_viability(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
    instrument_name: str | None = None,
) -> SuspensionMeasurementFields.Viability | None:
    if value := row[value_key]:
        # Divide by 100 because these values are formatted in a reasonable way (without the percent-sign) so they won't automatically be converted to a decimal inside str_to_float
        value = str_to_float(value) / 100
    else:
        return None

    return SuspensionMeasurementFields.Viability(
        measured_at=measured_at, instrument_name=instrument_name, value=value
    )


def _parse_cell_or_nucleus_diameter(
    row: dict[str, Any],
    value_key: str,
    measured_at: datetime,
    biological_material: BiologicalMaterial | None = None,
    instrument_name: str | None = None,
) -> SuspensionMeasurementFields.MeanDiameter | None:
    if value := row[value_key]:
        value = str_to_float(value)
    else:
        return None

    if biological_material is None:
        biological_material = BiologicalMaterial(
            to_snake_case(row["biological_material"])
        )

    unit = (biological_material, LengthUnit.Micrometer)

    return SuspensionMeasurementFields.MeanDiameter(
        measured_at=measured_at, instrument_name=instrument_name, unit=unit, value=value
    )


def f():
    measurements = []

    cell_counter = row["cell_counter"]
    if date_created := row["date_created"]:
        measured_at = date_str_to_eastcoast_9am(date_created)
    else:
        measured_at = parent_specimen.info.summary.received_at

    measured_by_for_customer_measurement = parent_specimen.info.submitted_by.id
    measured_by_for_scbl_measurement = data["preparer_ids"][0]

    concentrations = [
        (
            "customer_cell/nucleus_concentration_(cell-nucleus/ml)",
            None,
            None,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell/nucleus_concentration_(post-adjustment)_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "post-hybridization_cell/nucleus_concentration_(cell-nucleus/ml)",
            cell_counter,
            row["counting_method"],
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for (
        key,
        instrument_name,
        counting_method,
        measured_by,
        is_post_probe_hybridization,
    ) in concentrations:
        if measurement_data := _parse_concentration(
            row,
            value_key=key,
            instrument_name=instrument_name,
            counting_method=counting_method,
            measured_at=measured_at,
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_probe_hybridization=is_post_probe_hybridization,
            )
            data["measurements"].append(measurement)

    volumes = [
        (
            "customer_volume_(µl)",
            measured_by_for_customer_measurement,
            False,
        ),
        ("scbl_volume_(µl)", measured_by_for_scbl_measurement, False),
        ("scbl_volume_(post-adjustment)_(µl)", measured_by_for_scbl_measurement, False),
        (
            "post-hybridization_volume_(µl)",
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, measured_by, is_post_probe_hybridization in volumes:
        if measurement_data := _parse_volume(
            row, value_key=key, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_probe_hybridization=is_post_probe_hybridization,
            )
            data["measurements"].append(measurement)

    viabilities = [
        (
            "customer_cell_viability_(%)",
            None,
            measured_by_for_customer_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(%)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_cell_viability_(post-adjustment)_(%)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
    ]
    for key, instrument_name, measured_by, is_post_probe_hybridization in viabilities:
        if measurement_data := _parse_viability(
            row, value_key=key, instrument_name=instrument_name, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_probe_hybridization=is_post_probe_hybridization,
            )
            data["measurements"].append(measurement)

    diameters = [
        (
            "scbl_average_cell/nucleus_diameter_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_average_cell/nucleus_diameter_(post-adjustment)_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            False,
        ),
        (
            "scbl_post-hybridization_average_cell/nucleus_diameter_(µm)",
            cell_counter,
            measured_by_for_scbl_measurement,
            True,
        ),
    ]
    for key, instrument_name, measured_by, is_post_probe_hybridization in diameters:
        if measurement_data := _parse_cell_or_nucleus_diameter(
            row, value_key=key, instrument_name=instrument_name, measured_at=measured_at
        ):
            measurement = NewSuspensionMeasurement(
                measured_by=measured_by,
                data=measurement_data,
                is_post_probe_hybridization=is_post_probe_hybridization,
            )
            data["measurements"].append(measurement)
