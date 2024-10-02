package org.mobilitydata.gtfsvalidator.table;

import org.mobilitydata.gtfsvalidator.annotation.GtfsJson;

/**
 * This class contains the information from one feature in the geojson file. Note that currently no
 * class is autogenerated from this schema, contrarily to csv based entities.
 */
@GtfsJson("locations.geojson")
public interface GtfsGeojsonFeatureSchema extends GtfsEntity {

  String featureId();
}
