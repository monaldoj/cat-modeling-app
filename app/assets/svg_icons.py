def svg_pin_icon(color="blue"):
    svg = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40">
        <!-- Ellipse shadow -->
        <ellipse cx="20" cy="37" rx="10" ry="4" fill="rgba(0,0,0,0.25)" />
        
        <!-- Pin body (teardrop shape) -->
        <path d="M20 5
                 C28 5, 35 12, 35 20
                 C35 28, 20 38, 20 38
                 C20 38, 5 28, 5 20
                 C5 12, 12 5, 20 5 Z"
              fill="{color}" stroke="black" stroke-width="1.5"/>
        
        <!-- Center circle -->
        <circle cx="20" cy="20" r="5" fill="white" stroke="black" stroke-width="1"/>
    </svg>
    '''
    import base64
    encoded = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{encoded}"