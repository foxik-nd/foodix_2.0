import { useCameraPermissions, CameraView } from 'expo-camera';
import React, { useRef, useState, useEffect } from 'react';
import { View, Text, StyleSheet, ActivityIndicator } from 'react-native';
import { commonStyles } from '../styles';
import AsyncStorage from '@react-native-async-storage/async-storage';

export default function BarcodeScannerScreen({ navigation, route }) {
  const [permission, requestPermission] = useCameraPermissions();
  const [scanned, setScanned] = useState(false);

  const targetSlot = route.params?.target;
  const returnTo   = route.params?.returnTo; 

  const codesRef = useRef({
    code1: route.params?.code1 ?? null,
    code2: route.params?.code2 ?? null,
  });

  useEffect(() => {
    if (!permission) return;
    if (!permission.granted) requestPermission();
  }, [permission]);

  const handleBarcodeScanned = ({ data }) => {
    if (scanned) return;
    setScanned(true);

    if (targetSlot === 1) codesRef.current.code1 = data;
    if (targetSlot === 2) codesRef.current.code2 = data;

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

  const fetchProduct = async (barcode) => {
    const token = await AsyncStorage.getItem('token');
    const response = await fetch(`http://localhost:8000/product/${barcode}`, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    return response.json();
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
        onBarcodeScanned={handleBarcodeScanned}
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
