package dev.a4i.bsc.etl.common.util

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder

object FeatureTypeExtensions:

  extension (featureType: SimpleFeatureType)
    def renameAttributes(mapping: Map[String, String]): SimpleFeatureType =
      val featureTypeBuilder: SimpleFeatureTypeBuilder = SimpleFeatureTypeBuilder()

      featureTypeBuilder.setName(featureType.getName)
      featureTypeBuilder.setCRS(featureType.getCoordinateReferenceSystem)

      featureType.getAttributeDescriptors.forEach: originalAttributeDescriptor =>
        mapping.get(originalAttributeDescriptor.getLocalName) match
          case None                   => featureTypeBuilder.add(originalAttributeDescriptor)
          case Some(newAttributeName) =>
            val attributeTypeBuilder: AttributeTypeBuilder = new AttributeTypeBuilder:
              init(originalAttributeDescriptor)

            val renamedAttributeDescriptor: AttributeDescriptor =
              attributeTypeBuilder.buildDescriptor(newAttributeName, originalAttributeDescriptor.getType)

            featureTypeBuilder.add(renamedAttributeDescriptor)

      featureTypeBuilder.buildFeatureType
