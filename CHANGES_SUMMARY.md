# Summary of Changes Made

## Issue 1: Address field not populated after confirming marker on map

### Problem
When clicking the "подтвердить выбор" button in the map modal, the address field was not being populated with the address where the marker was placed.

### Solution
Modified the `getAddressFromCoords` function to dispatch input and change events after updating the address fields programmatically. This ensures that:

1. The UI properly reflects the new address values in the input fields
2. Any event listeners monitoring changes to the input fields are triggered
3. The address fields visually update to show the location where the marker was placed on the map

### Files Changed
- `/workspace/main.html`

## Issue 2: Marker position not updating when dragging

### Problem
When dragging the marker, the geoposition was not updating with the marker position.

### Solution
1. Added `draggable: true` property to the `fullscreenMarker` initialization
2. Added a `dragend` event handler that updates the state of the confirm button when dragging the marker
3. Implemented proper geocoding and address updating when the marker is dragged

### Files Changed
- `/workspace/main.html`

## Technical Details

The changes ensure that both clicking on the map and dragging the marker properly update the address field and maintain synchronization between the UI and the underlying data model.