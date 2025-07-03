import React, { useEffect, useState } from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs';
import { Ionicons } from '@expo/vector-icons';
import AsyncStorage from '@react-native-async-storage/async-storage';

import HomeScreen from './components/HomeScreen';
import BarcodeScannerScreen from './components/BarcodeScannerScreen';
import ComparaisonScreen from './components/ComparaisonScreen';
import ComparaisonResultScreen from './components/ComparaisonResultScreen';
import LoginScreen from './components/LoginScreen';

const Stack = createNativeStackNavigator();
const Tab   = createBottomTabNavigator();

function MainTabs() {
  return (
    <Tab.Navigator
      screenOptions={({ route }) => ({
        tabBarStyle:        { backgroundColor: '#FF9100', borderTopWidth: 0 },
        tabBarActiveTintColor: '#fff',
        tabBarInactiveTintColor: '#fff',
        headerShown: false,
        tabBarIcon: ({ color, size }) => {
          let iconName;
          if (route.name === 'Accueil')      iconName = 'home';
          if (route.name === 'Comparaison') iconName = 'bar-chart';
          return <Ionicons name={iconName} size={size} color={color} />;
        },
        tabBarLabelStyle: { fontSize: 12, fontWeight: 'bold' },
      })}
    >
      <Tab.Screen name="Accueil"      component={HomeScreen} />
      {}
      <Tab.Screen
        name="Comparaison"
        component={ComparaisonScreen}
        options={{ unmountOnBlur: false }}
      />
    </Tab.Navigator>
  );
}

export default function App() {
  const [isAuth, setIsAuth] = useState(null);

  useEffect(() => {
    AsyncStorage.getItem('token').then(token => {
      setIsAuth(!!token);
    });
  }, []);

  if (isAuth === null) return null; // ou un splash screen

  return (
    <NavigationContainer>
      <Stack.Navigator
        initialRouteName={isAuth ? 'Home' : 'Login'}
        screenOptions={{
          headerStyle: { backgroundColor: '#FF9100' },
          headerTintColor: '#fff',
          headerTitleStyle: { fontWeight: 'bold' },
        }}
      >
        <Stack.Screen name="Login" component={LoginScreen} />
        <Stack.Screen name="Home" component={HomeScreen} />
        <Stack.Screen name="Main" component={MainTabs} options={{ headerShown: false }} />

        {}
        <Stack.Screen
          name="Scan"
          component={BarcodeScannerScreen}
          options={{ title: 'Scanner un produit', headerShown: false }}
        />

        {}
        <Stack.Screen
          name="ComparaisonResult"
          component={ComparaisonResultScreen}
          options={{ title: 'Résultat de la comparaison', headerShown: true }}
        />
      </Stack.Navigator>
    </NavigationContainer>
  );
}
