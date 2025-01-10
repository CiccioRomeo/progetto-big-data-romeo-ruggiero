import { DeckGL } from '@deck.gl/react';
import { Map } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { INITIAL_VIEW_STATE_DISTANCES, MAP_STYLE } from '../../constants';
import { lightingEffect } from '../../lighting';
import { createConnectionLayer } from './arcLayers';

import styles from '../../MapView.module.css';
import { useState } from 'react';

import { Distances } from '@/lib/types'; 

interface DistanceMapProps {
  data: Distances[]; // Aggiunto il tipo Distances[]
}

export function ArcMapView({ data }: DistanceMapProps) {
  // Stato per cluster selezionati
  const [selectedClusters, setSelectedClusters] = useState<Set<number>>(new Set(data.map(({ centroid }) => centroid.label)));

  // Funzione per attivare/disattivare un cluster
  const toggleCluster = (index: number) => {
    setSelectedClusters((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(index)) {
        newSet.delete(index);
      } else {
        newSet.add(index);
      }
      return newSet;
    });
  };

  // Creazione dei layer
  const layers = data.length > 0 && selectedClusters.size > 0
    ? [createConnectionLayer(data, selectedClusters)]
    : [];

  return (
    <div className={styles.container}>
      {(data.length > 0) && (
      <div className={styles.sidebar}>
        <h3>Filtra per Cluster</h3>
        {data.map(({ centroid }, index) => (
          <div key={index}>
            <input
              type="checkbox"
              id={`cluster-${centroid.label}`}
              checked={selectedClusters.has(centroid.label)}
              onChange={() => toggleCluster(centroid.label)}
            />
            <label htmlFor={`cluster-${centroid.label}`}>Cluster {centroid.label}</label>
          </div>
        ))}
      </div>)}
      <DeckGL
        initialViewState={INITIAL_VIEW_STATE_DISTANCES}
        controller={{
          dragRotate: true,
          doubleClickZoom: false,
        }}
        layers={layers}
        effects={[lightingEffect]}
        getTooltip={({ object }: { object: any | null | undefined }) => {
          if (!object) {
            return null;
          }
          if ('distance' in object) {
            return `Centroide ${object.label} - ${object.monumentName}\nDistanza: ${object.distance.toFixed(2)} km`;
          }
          if ('name' in object) {
            return `${object.name}`;
          }
          if ('latitude' in object && 'longitude' in object) {
            return `Lat: ${object.latitude.toFixed(6)}\nLng: ${object.longitude.toFixed(6)}`;
          }
          return null;
        }}
      >
        <Map reuseMaps mapStyle={MAP_STYLE} attributionControl={false} />
      </DeckGL>
    </div>
  );
}
