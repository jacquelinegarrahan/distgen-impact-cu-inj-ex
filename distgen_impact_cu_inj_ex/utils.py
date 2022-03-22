import numpy as np
from skimage.filters import sobel
from skimage.util import img_as_ubyte
from skimage.segmentation import watershed
from skimage.filters.rank import median
from skimage.morphology import disk
import os
import pint

def write_distgen_xy_dist(filename, image, resolution, resolution_units='m'):
    """
    Writes image data in distgen's xy_dist format
    
    Returns the absolute path to the file written
    
    """
    
    # Get width of each dimension
    widths = resolution * np.array(image.shape)
    
    center_y = 0
    center_x = 0
    
    # Form header
    header = f"""x {widths[1]} {center_x} [{resolution_units}]
y {widths[0]} {center_y}  [{resolution_units}]"""
    
    # Save with the correct orientation
    np.savetxt(filename, np.flip(image, axis=0), header=header, comments='')
    
    return os.path.abspath(filename)



def format_distgen_xy_dist(image, resolution, resolution_units):
    """
    Writes image data in distgen's xy_dist format
    
    Returns the absolute path to the file written
    
    """

    ureg = pint.UnitRegistry()
    
    # Get width of each dimension
    widths = resolution * np.array(image.shape)

    
    center_y = 0
    center_x = 0

    x_min = center_x - widths[1]/2
    x_max = center_x + widths[1]/2
    y_min = center_y - widths[0]/2
    y_max = center_y + widths[0]/2
    
    image = image * ureg(resolution_units)

    return {
            'type' : 'image2d',
            'min_x': {'value':x_min, 'units':'mm'},
            'max_x': {'value':x_max, 'units':'mm'},
            'min_y': {'value':y_min, 'units':'mm'},
            'max_y': {'value':y_max, 'units':'mm'},
            'P': image
        }



def isolate_image(img, fclip=0.08):
    """
    Uses a masking technique to isolate the VCC image
    """
    img=img.copy()
    
    # Clip lowest fclip fraction
    img[img < np.max(img)* fclip] = 0
    
    # Filter out hot pixels to use as a mask
    # https://scikit-image.org/docs/0.12.x/auto_examples/xx_applications/plot_rank_filters.html
    img2 = median(img_as_ubyte(img), disk(2))
    
    elevation_map = sobel(img2)
    markers = np.zeros_like(img2)
    
    # TODO: tweak these numbers
    markers[img2 < .1] = 1
    markers[img2 > .2] = 2

    # Wateshed
    segmentation = watershed(elevation_map, markers)
    
    # Set to zero in original image
    img[np.where(segmentation != 2)]  = 0 
    
    # 
    ixnonzero0 = np.nonzero(np.sum(img2, axis=1))[0]
    ixnonzero1 = np.nonzero(np.sum(img2, axis=0))[0]
 
    i0, i1, j0, j1 = ixnonzero0[0], ixnonzero0[-1], ixnonzero1[0], ixnonzero1[-1]
    cutimg = img[i0:i1,j0:j1]
    
    return cutimg

