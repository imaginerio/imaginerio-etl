# imagineRio updater

## Running updates

There are two ways of updating imagineRio's contents: by pushing new data or manually triggering the workflow.

### Pushing data

Open [imagineRio's data repository](https://github.com/imaginerio/imaginerio-data). On the main page, press "." on your keyboard. This will open the online editor:

ONLINE EDITOR SCREENSHOT

#### Adding KMLs 
Navigate to the `input/kmls` folder in the left panel, then drag-and-drop your new KML files into it.

#### Adding metadata files
Navigate to the `input` folder in the left panel, and drop your `jstor.xls` and `vocabulary.xls` files into it. It is important that the files are named exactly as above, including capitalization.

#### Consolidate your changes
On the far-left of the screen there are tabs with icons. Hovering over the icons will show the tabs name. Go to "Source Control" (the icon with dots connected by lines). 

In the text field at the top, you must enter a message explaining what changes you are commiting. Good examples are short and descriptive, such as *"Added Thomas Ender kmls"* or *"Updated metadata files"*. After entering your message, click on "Commit & Push".

That's it! The update will be triggered automatically.