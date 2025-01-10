export interface Monument {
    monumento: string;
    latitudine: number;
    longitudine: number;
  }
  
  export interface Centroide {
    latitudine: number;
    longitudine: number;
    label: number;
  }
  
  export interface Distanza extends Monument {
    distanza: number;
  }
  
  export interface MockData {
    centroide: Centroide;
    distanze: Distanza[];
  }
  
  export function generateMockData(): MockData[] {
    const monumentsInRome: Monument[] = [
      { monumento: "Colosseo", latitudine: 41.8902, longitudine: 12.4922 },
      { monumento: "Foro Romano", latitudine: 41.8925, longitudine: 12.4853 },
      { monumento: "Pantheon", latitudine: 41.8986, longitudine: 12.4769 },
      { monumento: "Fontana di Trevi", latitudine: 41.9009, longitudine: 12.4833 },
      { monumento: "Piazza Navona", latitudine: 41.8992, longitudine: 12.4731 },
      { monumento: "Basilica di San Pietro", latitudine: 41.9022, longitudine: 12.4539 },
      { monumento: "Musei Vaticani", latitudine: 41.9065, longitudine: 12.4536 },
      { monumento: "Castel Sant'Angelo", latitudine: 41.9031, longitudine: 12.4663 },
      { monumento: "Piazza di Spagna", latitudine: 41.9057, longitudine: 12.4823 },
      { monumento: "Galleria Borghese", latitudine: 41.9142, longitudine: 12.4923 },
      { monumento: "Altare della Patria", latitudine: 41.8955, longitudine: 12.4823 },
      { monumento: "Circo Massimo", latitudine: 41.8855, longitudine: 12.4853 },
      { monumento: "Terme di Caracalla", latitudine: 41.8786, longitudine: 12.4922 },
      { monumento: "Piazza del Popolo", latitudine: 41.9109, longitudine: 12.4763 },
      { monumento: "Villa Borghese", latitudine: 41.9122, longitudine: 12.4924 },
      { monumento: "Campo de' Fiori", latitudine: 41.8959, longitudine: 12.4722 },
      { monumento: "Basilica di Santa Maria Maggiore", latitudine: 41.8978, longitudine: 12.4989 },
      { monumento: "Piazza Venezia", latitudine: 41.8963, longitudine: 12.4827 },
      { monumento: "Via del Corso", latitudine: 41.9031, longitudine: 12.4797 },
    ];
    
  
    const centroids: Centroide[] = Array.from({ length: 6}, (_, i) => ({
      latitudine: Math.random() * (41.92 - 41.85) + 41.85,
      longitudine: Math.random() * (12.50 - 12.45) + 12.45,
      label: i
    }));
  
    const calculateDistance = (lat1: number, lon1: number, lat2: number, lon2: number): number => {
      const R = 6371;
      const dLat = ((lat2 - lat1) * Math.PI) / 180;
      const dLon = ((lon2 - lon1) * Math.PI) / 180;
      const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos((lat1 * Math.PI) / 180) *
          Math.cos((lat2 * Math.PI) / 180) *
          Math.sin(dLon / 2) * Math.sin(dLon / 2);
      const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
      return R * c;
    };
  
    return centroids.map(centroid => {
      const distanze: Distanza[] = monumentsInRome.map(monument => ({
        ...monument,
        distanza: parseFloat(
          calculateDistance(
            centroid.latitudine,
            centroid.longitudine,
            monument.latitudine,
            monument.longitudine
          ).toFixed(2)
        )
      }));
  
      return { centroide: centroid, distanze };
    });
  }
