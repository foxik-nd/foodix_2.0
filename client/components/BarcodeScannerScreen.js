import { useCameraPermissions, CameraView } from 'expo-camera';
import React, { useRef, useState, useEffect } from 'react';
import { View, Text, StyleSheet, ActivityIndicator, Alert } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { API_BASE_URL } from '../config';
import { commonStyles } from '../styles';

export default function BarcodeScannerScreen({ navigation, route }) {
  const [permission, requestPermission] = useCameraPermissions();
  const [scanned, setScanned] = useState(false);

  const targetSlot = route.params?.target;
  const returnTo = route.params?.returnTo;

  const codesRef = useRef({
    code1: route.params?.code1 ?? null,
    code2: route.params?.code2 ?? null,
  });
  

  useEffect(() => {
    if (!permission) return;
    if (!permission.granted) requestPermission();
  }, [permission]);

  const fetchProduct = async (barcode) => {
    const token = await AsyncStorage.getItem('token');
    console.log("TOKEN STOCKÉ :", token);
    if (!token) throw new Error('Token manquant');

    const response = await fetch(`${API_BASE_URL}/product/${barcode}`, {
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error(`Erreur API (${response.status})`);
    }

    return response.json();
  };

  const handleBarcodeScanned = async ({ data }) => {
    if (scanned) return;
    setScanned(true);

    if (targetSlot === 1) codesRef.current.code1 = data;
    if (targetSlot === 2) codesRef.current.code2 = data;

    try {
      const product = await fetchProduct(data);
      console.log('Produit reçu :', product);
      // Tu peux ici stocker les infos du produit ou les passer à l’écran suivant si nécessaire
    } catch (error) {
      console.error('Erreur API :', error);
      Alert.alert('Erreur', 'Impossible de récupérer les infos du produit (authentification requise ?)');
      navigation.replace('Login'); // Optionnel : redirection si 401
      return;
    }

    setTimeout(() => {
      if (returnTo === 'Comparaison') {
        navigation.navigate('Main', {
          screen: 'Comparaison',
          params: {
            code1: codesRef.current.code1,
            code2: codesRef.current.code2,
          },
        });
      } else {
        navigation.navigate('Main', {
          screen: 'Accueil',
          params: { code: data },
        });
      }
    }, 500);
  };

  if (!permission) return <View />;
  if (!permission.granted) {
    return (
      <View style={commonStyles.container}>
        <Text style={styles.message}>L'application a besoin d'accéder à la caméra.</Text>
      </View>
    );
  }

  return (
    <View style={styles.cameraContainer}>
      <CameraView
        style={StyleSheet.absoluteFill}
        onBarcodeScanned={scanned ? undefined : handleBarcodeScanned}
        barcodeScannerSettings={{
          barcodeTypes: ['ean13', 'ean8', 'upc_a', 'upc_e'],
        }}
      />
      <View
        style={[
          styles.scanBox,
          { borderColor: scanned ? 'green' : 'orange' },
        ]}
      />
      {scanned && (
        <ActivityIndicator size="large" color="#FF9100" style={styles.loader} />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  cameraContainer: { flex: 1, position: 'relative', backgroundColor: 'black' },
  scanBox: {
    position: 'absolute',
    top: '30%',
    left: '15%',
    width: '70%',
    height: 200,
    borderWidth: 3,
    borderRadius: 12,
  },
  loader: { position: 'absolute', bottom: 40, alignSelf: 'center' },
  message: { textAlign: 'center', padding: 20, fontSize: 16, color: '#4E2C00' },
});
