document.addEventListener('DOMContentLoaded', function() {
    console.log('Web integration script initializing...');
    setTimeout(initIntegration, 500);
});

function initIntegration() {
    console.log('Starting initialization process');
    
    // Все необходимые элементы для интеграции
    const elements = {
        orderButton: document.getElementById('orderButton'),
        startAddress: document.getElementById('startAddress'),
        endAddress: document.getElementById('endAddress'),
        startAddressFull: document.getElementById('startAddressFull'),
        endAddressFull: document.getElementById('endAddressFull'),
        routeLoader: document.getElementById('routeLoader'),
        routeDetails: document.getElementById('routeDetails'),
        priceDisplay: document.getElementById('priceDisplay'),
        distanceDisplay: document.getElementById('distanceDisplay'),
        estimatedTime: document.getElementById('estimatedTime'),
        calculatedValues: document.getElementById('calculated-values'),
        orderComment: document.getElementById('orderComment')
    };
    
    // Проверка наличия всех элементов
    const missingElements = Object.keys(elements).filter(key => !elements[key]);
    if (missingElements.length > 0) {
        console.warn('Не все элементы DOM готовы. Отсутствуют:', missingElements);
        setTimeout(initIntegration, 1000);
        return;
    }
    
    // Глобальные переменные
    let isRouteCalculated = false;
    let passengerCount = 1;
    let startCoordinates = null;
    let endCoordinates = null;
    
    // URL API сервера - ИСПРАВЛЕНО: убраны лишние пробелы!
    const API_BASE_URL = window.location.hostname === 'localhost' ? 
        'http://localhost:8004' : 'https://taxibarsnz24.ru';
    
    console.log('Используется API_BASE_URL:', API_BASE_URL);

    // Функция получения координат по адресу
    async function getCoordinatesByAddress(address) {
        try {
            const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(address)}`);
            const data = await response.json();
            if (data && data.length > 0) {
                return {
                    lat: parseFloat(data[0].lat),
                    lng: parseFloat(data[0].lon)
                };
            }
            console.warn('Не удалось найти координаты для адреса:', address);
            return null;
        } catch (error) {
            console.error('Ошибка при получении координат:', error);
            return null;
        }
    }

    // Настройка получения координат при изменении адресов
    function setupCoordinateFetching() {
        // Для адреса отправления
        if (elements.startAddressFull) {
            elements.startAddressFull.addEventListener('change', async () => {
                startCoordinates = await getCoordinatesByAddress(elements.startAddressFull.value);
                console.log('Координаты отправления обновлены:', startCoordinates);
            });
        }
        
        if (elements.startAddress) {
            elements.startAddress.addEventListener('change', async () => {
                startCoordinates = await getCoordinatesByAddress(elements.startAddress.value);
                console.log('Координаты отправления обновлены:', startCoordinates);
            });
        }
        
        // Для адреса назначения
        if (elements.endAddressFull) {
            elements.endAddressFull.addEventListener('change', async () => {
                endCoordinates = await getCoordinatesByAddress(elements.endAddressFull.value);
                console.log('Координаты назначения обновлены:', endCoordinates);
            });
        }
        
        if (elements.endAddress) {
            elements.endAddress.addEventListener('change', async () => {
                endCoordinates = await getCoordinatesByAddress(elements.endAddress.value);
                console.log('Координаты назначения обновлены:', endCoordinates);
            });
        }
    }
    
    // Сразу пытаемся получить координаты для уже заполненных адресов
    async function fetchInitialCoordinates() {
        const pickupAddress = (elements.startAddressFull.value || elements.startAddress.value).trim();
        const dropoffAddress = (elements.endAddressFull.value || elements.endAddress.value).trim();
        
        if (pickupAddress && !startCoordinates) {
            startCoordinates = await getCoordinatesByAddress(pickupAddress);
            console.log('Начальные координаты отправления:', startCoordinates);
        }
        
        if (dropoffAddress && !endCoordinates) {
            endCoordinates = await getCoordinatesByAddress(dropoffAddress);
            console.log('Начальные координаты назначения:', endCoordinates);
        }
    }
    
    // Инициализируем получение координат
    setupCoordinateFetching();
    fetchInitialCoordinates();

    // Функция обновления состояния кнопки заказа
    function updateOrderButtonState() {
        const start = (elements.startAddressFull.value || elements.startAddress.value).trim();
        const end = (elements.endAddressFull.value || elements.endAddress.value).trim();
        
        console.log('Проверка состояния кнопки:', {
            start: start,
            end: end,
            isRouteCalculated: isRouteCalculated
        });
        
        if (start && end && start.length > 3 && end.length > 3) {
            elements.orderButton.disabled = false;
            console.log('Кнопка "ЗАКАЗАТЬ" активирована');
            
            if (!isRouteCalculated && elements.routeDetails) {
                elements.routeDetails.textContent = "✅ Адреса заполнены. Маршрут рассчитывается...";
                elements.routeDetails.style.display = 'block';
                elements.routeDetails.style.color = '#2e7d32';
                elements.routeDetails.style.backgroundColor = 'rgba(46, 125, 50, 0.1)';
            }
        } else {
            elements.orderButton.disabled = true;
            console.log('Кнопка "ЗАКАЗАТЬ" неактивна: адреса не заполнены');
            
            let message = "Заполните оба адреса для оформления заказа";
            if (!start && !end) message = "Укажите адрес отправления и назначения";
            else if (!start) message = "Укажите адрес отправления";
            else if (!end) message = "Укажите адрес назначения";
            
            if (elements.routeDetails) {
                elements.routeDetails.textContent = message;
                elements.routeDetails.style.display = 'block';
                elements.routeDetails.style.color = '#d32f2f';
                elements.routeDetails.style.backgroundColor = 'rgba(211, 47, 47, 0.1)';
            }
            
            if (elements.calculatedValues) {
                elements.calculatedValues.style.display = 'none';
            }
        }
    }

    // Функция создания заказа через API
async function createOrderViaApi() {
    const userData = JSON.parse(localStorage.getItem('tg_user'));
    if (!userData) {
        alert('Пожалуйста, авторизуйтесь для создания заказа');
        window.location.href = 'login.html';
        return;
    }

    const pickupAddress = elements.startAddressFull.value.trim() || elements.startAddress.value.trim();
    const dropoffAddress = elements.endAddressFull.value.trim() || elements.endAddress.value.trim();
    const comment = elements.orderComment.value.trim() || '';
    const passengers = passengerCount || 1;
    
    let price = 150;
    let distance = 5.0;
    let estimatedTime = '15 минут';
    
    try {
        if (elements.priceDisplay.textContent.includes('₽')) {
            price = parseFloat(elements.priceDisplay.textContent.replace(/[^0-9.,]/g, '').replace(',', '.'));
        }
        if (elements.distanceDisplay.textContent.includes('км')) {
            distance = parseFloat(elements.distanceDisplay.textContent.replace(/[^0-9.,]/g, '').replace(',', '.'));
        }
        if (elements.estimatedTime.textContent && elements.estimatedTime.textContent !== 'Рассчитывается') {
            estimatedTime = elements.estimatedTime.textContent;
        }
        
        // Проверка корректности чисел
        if (isNaN(price) || price < 0) price = 150;
        if (isNaN(distance) || distance < 0) distance = 5.0;
    } catch (error) {
        console.warn('Ошибка при извлечении цены и расстояния:', error);
    }
    
    // 🔥 КРИТИЧЕСКИ ВАЖНО: Сохраняем данные заказа во временное хранилище
    const tempOrderData = {
        pickup_address: pickupAddress,
        dropoff_address: dropoffAddress,
        price: price,
        distance_km: distance,
        estimated_time_min: estimatedTime,
        passengers: passengers,
        comment: comment,
        timestamp: Date.now()
    };
    localStorage.setItem('last_order_data', JSON.stringify(tempOrderData));
    
    // Валидация данных
    if (!pickupAddress || !dropoffAddress) {
        alert('Пожалуйста, заполните оба адреса для заказа такси');
        return;
    }
    
    // Получаем координаты если они еще не получены
    if (!startCoordinates) {
        startCoordinates = await getCoordinatesByAddress(pickupAddress);
    }
    if (!endCoordinates) {
        endCoordinates = await getCoordinatesByAddress(dropoffAddress);
    }

    try {
        elements.orderButton.disabled = true;
        elements.routeLoader.classList.add('active');
        elements.routeLoader.textContent = 'Создание заказа...';
        elements.routeLoader.style.display = 'block';
        
        const response = await fetch(`${API_BASE_URL}/api/web/order/create`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                client_id: userData.id,
                pickup_address: pickupAddress,
                dropoff_address: dropoffAddress,
                comment: comment,
                passengers: passengers,
                price: price,
                distance_km: distance,
                estimated_time_min: estimatedTime,
                pickup_lat: startCoordinates ? startCoordinates.lat : null,
                pickup_lon: startCoordinates ? startCoordinates.lng : null,
                dropoff_lat: endCoordinates ? endCoordinates.lat : null,
                dropoff_lon: endCoordinates ? endCoordinates.lng : null
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => null);
            throw new Error(`Ошибка сервера: ${response.status} ${errorData?.detail || response.statusText}`);
        }
        
        const result = await response.json();
        
        if (result.success && result.order_id) {
            // 🔥 Сохраняем полные данные заказа для восстановления
            const activeOrderData = {
                orderId: result.order_id,
                pickupAddress: pickupAddress,
                dropoffAddress: dropoffAddress,
                price: price,
                distanceKm: distance,
                estimatedTime: estimatedTime,
                passengers: passengers,
                driverName: 'Водитель',
                createdAt: new Date().toISOString()
            };
            localStorage.setItem('activeOrderData', JSON.stringify(activeOrderData));
            
            console.log('Заказ успешно создан, ID:', result.order_id);
            showDriverModalForWeb(result.order_id, {
                pickup_address: pickupAddress,
                dropoff_address: dropoffAddress,
                price: price,
                distance_km: distance,
                estimated_time_min: estimatedTime,
                passengers: passengers
            });
        } else {
            throw new Error(result.message || 'Ошибка при создании заказа');
        }
    } catch (error) {
        console.error('Ошибка при создании заказа:', error);
        alert(`Ошибка при создании заказа: ${error.message}`);
    } finally {
        elements.orderButton.disabled = false;
        elements.routeLoader.classList.remove('active');
        elements.routeLoader.style.display = 'none';
    }
}
    
    // Показать модальное окно выбора водителя
function showDriverModalForWeb(orderId, orderData) {
    // Проверяем, не открыто ли уже модальное окно
    if (isDriverModalOpen) return;
    
    isDriverModalOpen = true;
    const driverModal = document.getElementById('driverModal');
    const driversList = document.querySelector('.drivers-list');
    const timerEl = document.getElementById('timerValue');
    
    if (!driverModal || !driversList || !timerEl) {
        console.error('Не найдены необходимые элементы для модального окна выбора водителя');
        isDriverModalOpen = false;
        return;
    }
    
    // Очищаем список водителей и показываем состояние ожидания
    driversList.innerHTML = `
        <div class="driver-card" style="justify-content: center; text-align: center; padding: 20px;">
            <div class="search-spinner" style="border: 3px solid rgba(0,0,0,0.1); border-top: 3px solid var(--primary-color); border-radius: 50%; width: 30px; height: 30px; margin: 0 auto 15px; animation: spin 1s linear infinite;"></div>
            <p>Ожидание откликов водителей...</p>
        </div>
    `;
    
    // Показываем модальное окно
    driverModal.classList.add('active');
    document.body.style.overflow = 'hidden';
    
    // Запускаем таймер (2 минуты)
    let timeLeft = 120;
    timerEl.textContent = '02:00';
    
    // Очищаем предыдущий интервал, если он существует
    if (driverTimerInterval) clearInterval(driverTimerInterval);
    
    // Создаем новый интервал для таймера
    driverTimerInterval = setInterval(() => {
        timeLeft--;
        const minutes = Math.floor(timeLeft / 60).toString().padStart(2, '0');
        const seconds = (timeLeft % 60).toString().padStart(2, '0');
        timerEl.textContent = `${minutes}:${seconds}`;
        
        if (timeLeft <= 0) {
            clearInterval(driverTimerInterval);
            closeDriverModal();
            showCancelScreen();
        }
    }, 1000);
    
    // Функция опроса откликов водителей
    const pollBids = async () => {
        try {
            const API_BASE_URL = window.location.hostname === 'localhost' 
                ? 'http://localhost:8004' 
                : 'https://taxibarsnz24.ru';
            
            const res = await fetch(`${API_BASE_URL}/api/web/order/${orderId}/bids`);
            
            if (!res.ok) {
                throw new Error(`Ошибка сервера: ${res.status} ${await res.text()}`);
            }
            
            const data = await res.json();
            
            if (data.success && data.bids?.length) {
                renderDriverBids(data.bids, orderId, orderData);
            }
            
            // Продолжаем опрашивать, пока окно открыто
            if (isDriverModalOpen) {
                setTimeout(pollBids, 2000);
            }
        } catch (error) {
            console.error('Ошибка при получении откликов водителей:', error);
            
            // Показываем сообщение об ошибке в интерфейсе
            if (isDriverModalOpen && driversList) {
                driversList.innerHTML = `
                    <div class="status-notification error" style="margin: 10px;">
                        <div class="notification-icon">
                            <i class="fas fa-exclamation-triangle"></i>
                        </div>
                        <div class="notification-text">
                            Ошибка при получении откликов. Повторная попытка через 3 секунды...
                        </div>
                    </div>
                `;
            }
            
            // Продолжаем опрашивать, даже при ошибке
            if (isDriverModalOpen) {
                setTimeout(pollBids, 3000);
            }
        }
    };
    
    // Запускаем опрос откликов
    pollBids();
    
    // Функция закрытия модального окна
    window.closeDriverModal = function() {
        if (!isDriverModalOpen) return;
        
        isDriverModalOpen = false;
        
        if (driverTimerInterval) {
            clearInterval(driverTimerInterval);
            driverTimerInterval = null;
        }
        
        if (driverModal) {
            driverModal.classList.remove('active');
        }
        
        document.body.style.overflow = 'auto';
    };
}
    
    // Отобразить отклики водителей
    function renderDriverBids(bids, orderId) {
        const driversList = document.querySelector('.drivers-list');
        
        if (!driversList) {
            console.error('Не найден элемент для списка водителей');
            return;
        }

        driversList.innerHTML = '';
        
        bids.forEach(bid => {
            const driverCard = document.createElement('div');
            driverCard.className = 'driver-card';
            driverCard.dataset.driverId = bid.driver_id;
            
            const initials = bid.driver_name ? bid.driver_name.charAt(0) : 'В';
            
            driverCard.innerHTML = `
                <div class="driver-card-avatar">${initials}</div>
                <div class="driver-card-info">
                    <div class="driver-card-name">${bid.driver_name || `Водитель #${bid.driver_id}`}</div>
                    <div class="driver-car">${bid.car_brand} • ${bid.car_number}</div>
                    <div style="font-size: 14px; font-weight: 500; color: var(--primary-color); margin-top: 6px;">
                        Прибытие: ${bid.arrival_minutes} мин
                    </div>
                </div>
                <button class="driver-select-btn">Выбрать</button>
            `;
            
            driverCard.querySelector('.driver-select-btn').addEventListener('click', () => {
                selectDriverForOrder(orderId, bid.driver_id);
            });
            
            driversList.appendChild(driverCard);
        });
    }
    
    // Выбрать водителя для заказа
    async function selectDriverForOrder(orderId, driverId) {
        try {
            elements.routeLoader.classList.add('active');
            elements.routeLoader.textContent = 'Подтверждение выбора...';
            elements.routeLoader.style.display = 'block';
            
            const response = await fetch(`${API_BASE_URL}/api/web/order/${orderId}/accept`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ driver_id: driverId })
            });
            
            if (!response.ok) {
                const errorData = await response.json().catch(() => null);
                throw new Error(`Ошибка сервера: ${response.status} ${errorData?.detail || response.statusText}`);
            }
            
            const result = await response.json();
            
            if (result.success) {
                if (window.closeDriverModal) window.closeDriverModal();
                showOrderAcceptedScreenWeb(result.order_details);
            } else {
                throw new Error(result.message || 'Ошибка при выборе водителя');
            }
        } catch (error) {
            console.error('Ошибка при выборе водителя:', error);
            alert(`Ошибка при выборе водителя: ${error.message}`);
        } finally {
            elements.routeLoader.classList.remove('active');
            elements.routeLoader.style.display = 'none';
        }
    }
    
    // Показать экран с подтвержденным заказом
    function showOrderAcceptedScreenWeb(orderDetails) {
        // Сохраняем заказ в историю
        const newOrder = {
            id: `order_${Date.now()}`,
            date: new Date().toLocaleDateString('ru-RU', { 
                day: '2-digit', 
                month: '2-digit', 
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            }),
            from: elements.startAddressFull.value || elements.startAddress.value,
            to: elements.endAddressFull.value || elements.endAddress.value,
            price: elements.priceDisplay.textContent,
            distance: elements.distanceDisplay.textContent.replace(' км', ''),
            status: 'completed',
            driver: orderDetails.driver_name,
            order_id: orderDetails.order_id
        };
        
        saveOrderToHistory(newOrder);
        
        const appContainer = document.querySelector('.container');
        if (appContainer) {
            appContainer.style.display = 'none';
        }
        
        document.body.innerHTML = `
            <div class="order-container" style="max-width: 480px; width: 100%; background: var(--card-bg); border-radius: 16px; overflow: hidden; box-shadow: 0 2px 12px var(--shadow-light); position: relative; margin: 10px; display: flex; flex-direction: column; height: calc(100vh - 20px); max-height: 800px; border: 1px solid var(--border-color);">
                <div style="flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 20px; text-align: center;">
                    <div style="font-size: 64px; color: var(--primary-color); margin-bottom: 20px;">
                        <i class="fas fa-check"></i>
                    </div>
                    <h2 style="font-size: 24px; font-weight: 600; margin-bottom: 16px;">Заказ принят!</h2>
                    <p style="font-size: 16px; color: var(--text-secondary); margin-bottom: 24px; max-width: 320px;">
                        Водитель <strong>${orderDetails.driver_name}</strong> едет к вам. 
                        Прибытие через ${orderDetails.estimated_arrival}.
                    </p>
                    <div style="background: var(--background-light); border-radius: 16px; padding: 16px; width: 100%; max-width: 320px; margin-bottom: 24px;">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                            <span style="color: var(--text-secondary); font-size: 14px;">Стоимость поездки:</span>
                            <span style="font-weight: 600; color: var(--text-color); font-size: 18px;">${orderDetails.price || elements.priceDisplay.textContent}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span style="color: var(--text-secondary); font-size: 14px;">Расстояние:</span>
                            <span style="font-weight: 500; color: var(--text-color);">${elements.distanceDisplay.textContent}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; margin-top: 8px; padding-top: 8px; border-top: 1px solid var(--border-color);">
                            <span style="color: var(--text-secondary); font-size: 14px;">Номер заказа:</span>
                            <span style="font-weight: 500; color: var(--primary-color);">#${orderDetails.order_id}</span>
                        </div>
                    </div>
                </div>
                <div class="order-footer" style="padding: 16px 20px 20px; border-top: 1px solid var(--border-color); background: var(--card-bg);">
                    <button class="action-btn finish-btn" id="backToMainBtn" style="background: var(--primary-color); color: white; padding: 16px; border-radius: 14px; font-size: 16px; font-weight: 600; width: 100%; border: none; cursor: pointer;">
                        <i class="fas fa-home"></i>
                        <span>НА ГЛАВНУЮ</span>
                    </button>
                </div>
            </div>
            <style>
                :root {
                    --primary-color: #000;
                    --background-light: #f8f9fa;
                    --card-bg: #ffffff;
                    --border-color: #d9d9d9;
                    --text-color: #212121;
                    --text-secondary: #616161;
                }
                body {
                    background-color: var(--background-light);
                    margin: 0;
                    padding: 0;
                    min-height: 100vh;
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                }
            </style>
        `;
        
        document.getElementById('backToMainBtn').addEventListener('click', () => {
            window.location.reload();
        });
    }
    
    // Функция сохранения заказа в историю
    function saveOrderToHistory(order) {
        const history = JSON.parse(localStorage.getItem('orderHistory')) || [];
        history.unshift(order);
        if (history.length > 10) history.pop();
        localStorage.setItem('orderHistory', JSON.stringify(history));
    }
    
    // Функция отображения экрана отмены
    function showCancelScreen() {
        const appContainer = document.querySelector('.container');
        if (appContainer) {
            appContainer.style.display = 'none';
        }
        
        document.body.innerHTML = `
            <div class="cancel-screen" style="max-width: 480px; width: 100%; background: var(--card-bg); border-radius: 16px; overflow: hidden; box-shadow: 0 2px 12px var(--shadow-light); position: relative; margin: 10px;">
                <div style="padding: 40px 20px; text-align: center;">
                    <div style="font-size: 48px; color: var(--error-color); margin-bottom: 20px;">
                        <i class="fas fa-times-circle"></i>
                    </div>
                    <h2 style="font-size: 24px; font-weight: 600; margin-bottom: 16px;">Заказ отменен</h2>
                    <p style="font-size: 16px; color: var(--text-secondary); margin-bottom: 30px;">
                        Время на выбор водителя истекло. Заказ автоматически отменен.
                    </p>
                    <button class="action-btn" onclick="location.reload()" style="background: var(--primary-color); color: white; padding: 16px; border-radius: 14px; font-size: 16px; font-weight: 600; width: 100%; border: none; cursor: pointer;">
                        <i class="fas fa-redo"></i>
                        <span>СОЗДАТЬ НОВЫЙ ЗАКАЗ</span>
                    </button>
                </div>
            </div>
            <style>
                :root {
                    --primary-color: #000;
                    --card-bg: #ffffff;
                    --error-color: #d32f2f;
                    --text-secondary: #616161;
                }
                body {
                    background-color: var(--background-light);
                    margin: 0;
                    padding: 0;
                    min-height: 100vh;
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                }
            </style>
        `;
    }
    
    // Заменяем оригинальные обработчики
    function replaceOriginalHandlers() {
        if (elements.orderButton) {
            // Удаляем все существующие обработчики
            const clone = elements.orderButton.cloneNode(true);
            elements.orderButton.parentNode.replaceChild(clone, elements.orderButton);
            elements.orderButton = document.getElementById('orderButton');
            
            // Добавляем новый обработчик
            elements.orderButton.addEventListener('click', (e) => {
                e.preventDefault();
                createOrderViaApi();
            });
            console.log('Обработчик кнопки заказа заменен');
        }
    }
    
    // Настройка обработчиков ввода
    function setupInputListeners() {
        const inputs = [
            elements.startAddress,
            elements.endAddress,
            elements.startAddressFull,
            elements.endAddressFull
        ];
        
        inputs.forEach(input => {
            if (input) {
                input.addEventListener('input', function() {
                    setTimeout(updateOrderButtonState, 100);
                });
            }
        });
    }
    
    // Запуск инициализации
    replaceOriginalHandlers();
    setupInputListeners();
    
    // Периодическая проверка состояния кнопки
    setInterval(updateOrderButtonState, 5000);
    
    // Первоначальная проверка
    setTimeout(updateOrderButtonState, 300);
    
    console.log('Web integration initialized successfully');
}