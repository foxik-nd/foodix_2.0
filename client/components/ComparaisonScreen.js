import { useEffect, useRef, useState } from 'react';
import {
  View,
  Text,
  TouchableOpacity,
  StyleSheet,
  ActivityIndicator,
  Dimensions,
} from 'react-native';
import axios from 'axios';
import { API_BASE_URL } from '../config';
import { useFocusEffect } from '@react-navigation/native';

const { width, height } = Dimensions.get('window');

export default function ComparaisonScreen({ navigation, route }) {
  const codesRef = useRef({ code1: null, code2: null });
  const [p1, setP1] = useState(null);
  const [p2, setP2] = useState(null);
  const [loading1, setLoading1] = useState(false);
  const [loading2, setLoading2] = useState(false);

  useFocusEffect(() => {
    const params = route.params ?? {};
    if (params.code1 && params.code1 !== codesRef.current.code1) {
      codesRef.current.code1 = params.code1;
      fetchProduct(params.code1, setP1, setLoading1);
    }
    if (params.code2 && params.code2 !== codesRef.current.code2) {
      codesRef.current.code2 = params.code2;
      fetchProduct(params.code2, setP2, setLoading2);
    }
  });

  const fetchProduct = async (code, setter, setLoading) => {
    try {
      setLoading(true);
      const res = await axios.get(`${API_BASE_URL}/product/${code}?essential=true`);
      setter(res.data);
    } catch {
      setter({
        product_name: 'Erreur de chargement',
        nutriscore: { grade: 'N/A', score: 'UNKNOWN' },
      });
    } finally {
      setLoading(false);
    }
  };

  const goScan = (slot) => {
    navigation.navigate('Scan', {
      returnTo: 'Comparaison',
      target: slot,
      code1: codesRef.current.code1,
      code2: codesRef.current.code2,
    });
  };

  const compare = () => {
    navigation.navigate('ComparaisonResult', {
      product1: p1,
      product2: p2,
    });
  };

  return (
    <View style={styles.container}>
      <View style={styles.triangleLeft} />
      <View style={styles.triangleRight} />

      <TouchableOpacity style={styles.produit1} onPress={() => goScan(1)}>
        {loading1 ? (
          <ActivityIndicator color="#000" />
        ) : (
          <Text style={styles.cardText}>
            {p1
              ? `🔄 ${p1.product_name}\nNutri-Score ${p1.nutriscore?.grade?.toUpperCase()}`
              : 'Scanner Produit 1'}
          </Text>
        )}
      </TouchableOpacity>

      <TouchableOpacity style={styles.produit2} onPress={() => goScan(2)}>
        {loading2 ? (
          <ActivityIndicator color="#000" />
        ) : (
          <Text style={styles.cardText}>
            {p2
              ? `🔄 ${p2.product_name}\nNutri-Score ${p2.nutriscore?.grade?.toUpperCase()}`
              : 'Scanner Produit 2'}
          </Text>
        )}
      </TouchableOpacity>

      {p1 && p2 && !loading1 && !loading2 ? (
        <TouchableOpacity style={styles.centerCircle} onPress={compare}>
          <Text style={styles.centerEmoji}>💥</Text>
        </TouchableOpacity>
      ) : (
        <View style={styles.vsContainer}>
          <Text style={styles.vsText}>VS</Text>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },

  triangleLeft: {
    position: 'absolute',
    top: 0,
    left: 0,
    width: 0,
    height: 0,
    borderTopWidth: height,
    borderRightWidth: width,
    borderTopColor: '#2F62BD',
    borderRightColor: 'transparent',
    zIndex: 0,
  },
  triangleRight: {
    position: 'absolute',
    top: 0,
    right: 0,
    width: 0,
    height: 0,
    borderBottomWidth: height,
    borderLeftWidth: width,
    borderBottomColor: '#D32F2F',
    borderLeftColor: 'transparent',
    zIndex: 0,
  },

  produit1: {
    position: 'absolute',
    top: '18%',
    left: '8%',
    backgroundColor: 'white',
    borderRadius: 12,
    padding: 16,
    elevation: 5,
    zIndex: 2,
    maxWidth: '70%',
  },
  produit2: {
    position: 'absolute',
    bottom: '12%',
    right: '8%',
    backgroundColor: 'white',
    borderRadius: 12,
    padding: 16,
    elevation: 5,
    zIndex: 2,
    maxWidth: '70%',
  },
  cardText: {
    fontWeight: 'bold',
    fontSize: 14,
    textAlign: 'center',
  },

  vsContainer: {
    position: 'absolute',
    top: height / 2 - 30,
    left: width / 2 - 30,
    backgroundColor: '#000',
    borderRadius: 30,
    width: 60,
    height: 60,
    justifyContent: 'center',
    alignItems: 'center',
    zIndex: 3,
    elevation: 6,
  },
  vsText: {
    fontSize: 20,
    fontWeight: 'bold',
    color: 'white',
  },

  centerCircle: {
    position: 'absolute',
    top: height / 2 - 30,
    left: width / 2 - 30,
    width: 60,
    height: 60,
    borderRadius: 30,
    backgroundColor: '#000',
    justifyContent: 'center',
    alignItems: 'center',
    zIndex: 3,
    elevation: 6,
  },
  centerEmoji: {
    fontSize: 28,
  },
});
